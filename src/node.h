#pragma once

#include <vector>
#include <atomic>
#include <memory>
#include <thread>
#include <unordered_map>

#include "shared.h"

struct Node;

inline void broadcast(int fd,const std::shared_ptr<Node>& node, const char *buffer, unsigned int buffer_size);

struct Node {
    const unsigned char node_id;
    const unsigned char leader_id;
    std::vector<Address> peers;
    const unsigned int buffer_size;
    const unsigned int log_size;
    const unsigned int quorum;
    const Address &address;
    const Address &client_listener;

    char **log;
    std::atomic<unsigned char> *acks;
    std::atomic<unsigned int> committed{};

    Node(
            const unsigned char id,
            const unsigned char leader_id,
            const Address& client_listener,
            const std::vector<Address> &peers,
            const unsigned int buffer_size,
            const unsigned int log_size
    ): node_id(id), leader_id(leader_id), peers(peers), buffer_size(buffer_size), log_size(log_size),
        quorum(static_cast<unsigned int>(peers.size()) / 2 + 1), address(peers[id]), client_listener(client_listener)
    {
        if (log_size == 0) {
            throw std::invalid_argument("log_size must be > 0");
        }

        if (id >= peers.size() || leader_id >= peers.size()) {
            throw std::invalid_argument("node_id is out of bounds");
        }

        log = new char*[log_size];
        acks = new std::atomic<unsigned char>[log_size];
        for (unsigned int i = 0; i < log_size; ++i) {
            acks[i].store(0);
        }
        committed.store(0);
    }

    ~Node() {
        delete[] log;
        delete[] acks;
    }
};

struct KvEntry {
    char* data;
    size_t size;
};

inline void leader_commit_upward(const std::shared_ptr<Node>& node, const int server_fd) {
    try {
        auto pool = new BufferPool(node->log_size*2, node->buffer_size);
        std::unordered_map<std::string, KvEntry> kv_store;

        for (unsigned int i = 0; i < node->log_size; ++i) {
            std::string key = "key" + std::to_string(i);
            const auto value = new char[node->buffer_size];
            std::memset(value, 'x', node->buffer_size);
            kv_store[key] = KvEntry{value, node->buffer_size};
        }

        unsigned int consumed = 0;
        auto temp_buffer = new char[100000];
        sockaddr sender_addr{};
        // std::memcpy(&sender_addr, &data[5], sizeof(sockaddr_in));
        while (RUNNING.load(std::memory_order_relaxed)) {
            if (node->committed.load(std::memory_order_relaxed) > consumed) {
                const auto data = node->log[consumed % node->log_size];
                if (data == nullptr) {
                    throw std::runtime_error("node commit failed data was null");
                }

                if (data[21] == REQUEST_WRITE) {
                    unsigned int keySize;
                    std::memcpy(&keySize, &data[22], sizeof(unsigned int));

                    char* keyBuffer = pool->acquire();
                    std::memcpy(keyBuffer, &data[22 + sizeof(unsigned int)], keySize);

                    unsigned int value_size;
                    std::memcpy(&value_size, &data[22 + sizeof(unsigned int) + keySize], sizeof(unsigned int));
                    const char* value_ptr = &data[22 + sizeof(unsigned int) + keySize + sizeof(unsigned int)];
                    char* valueBuffer = pool->acquire();
                    std::memcpy(valueBuffer, value_ptr, value_size);

                    std::string key_str(keyBuffer, keySize);
                    kv_store[key_str] = KvEntry{valueBuffer, value_size};

                    temp_buffer[0] = OP_CLIENT_RESPONSE;
                    std::memcpy(&sender_addr, &data[5], sizeof(sockaddr_in));
                    if (sendto(server_fd, temp_buffer, 1, 0, &sender_addr, sizeof(sockaddr)) <= 0) {
                        throw std::runtime_error(
                            "Failed to send message to node " + std::to_string(node->node_id));
                    }
                } else if (data[21] == REQUEST_READ) {
                    unsigned int keySize;
                    std::memcpy(&keySize, &data[22], sizeof(unsigned int));

                    char* keyBuffer = pool->acquire();
                    std::memcpy(keyBuffer, &data[22 + sizeof(unsigned int)], keySize);

                    std::string key_str(keyBuffer, keySize);

                    auto it = kv_store.find(key_str);
                    if (it != kv_store.end()) {
                        const KvEntry& entry = it->second;

                        temp_buffer[0] = OP_CLIENT_RESPONSE;

                        std::memcpy(&temp_buffer[1], &entry.size, sizeof(unsigned int));
                        std::memcpy(&temp_buffer[1 + sizeof(unsigned int)], entry.data, entry.size);

                        std::memcpy(&sender_addr, &data[5], sizeof(sockaddr_in));
                        if (sendto(server_fd, temp_buffer, 1 + sizeof(unsigned int) + entry.size, 0, &sender_addr, sizeof(sockaddr)) <= 0) {
                            throw std::runtime_error("Failed to send response to client");
                        }
                    } else {
                        throw std::invalid_argument("data apply failed!");
                    }

                    temp_buffer[0] = OP_CLIENT_RESPONSE;

                } else {
                    throw std::invalid_argument("Invalid leader address format");
                }

                consumed++;
            } else std::this_thread::yield();
        }
    } catch (std::exception &e) {
        std::cout << e.what() << std::endl;
    }
}

inline void leader_client_listener(const std::shared_ptr<Node>& node) {
    try {
        const auto request_fd = setup_server_socket(node->client_listener.host(), node->client_listener.port());
        unsigned int slot = 0;
        auto pool = new BufferPool(node->log_size, node->buffer_size);
        sockaddr_in client_addr{};
        auto *client_sockaddr = reinterpret_cast<sockaddr *>(&client_addr);
        socklen_t cli_addr_len = sizeof(client_addr);
        while (RUNNING.load(std::memory_order_relaxed)) {
            // std::cout << "Still looping 3!" << std::endl;
            const auto buffer = pool->acquire();
            cli_addr_len = sizeof(client_addr);
            if (const auto size = recvfrom(request_fd, buffer, node->buffer_size, 0, client_sockaddr, &cli_addr_len); size > 0) {
                if (buffer[0] == OP_CLIENT_REQUEST) {
                    const auto next_slot = slot++;
                    if (node->acks[next_slot % node->log_size] != 0) {
                        throw std::runtime_error(
                                "OUT OF LOG SPACE AT INDEX: " + std::to_string(next_slot) + " " +
                                std::to_string(node->acks[next_slot % node->log_size]));
                    }
                    node->acks[next_slot % node->log_size].store(1);
                    std::memcpy(&buffer[1], &next_slot, sizeof(int));
                    std::memcpy(&buffer[5], &client_addr, cli_addr_len);
                    node->log[next_slot % node->log_size] = buffer;
                    buffer[0] = OP_PROPOSE;
                    broadcast(request_fd, node, buffer, size);
                } else {
                    throw std::invalid_argument(
                            "Invalid op on client request: " + std::to_string(buffer[0]));
                }
            } else {
                pool->release(buffer);
            }
        }

        delete pool;
        ::close(request_fd);
    } catch (std::exception &e) {
        std::cout << e.what() << std::endl;
    }
}

inline void peer_listener(const std::shared_ptr<Node>& node, const int server_fd) {
    try {
        char ack_buffer[5];
        ack_buffer[0] = OP_ACK;
        sockaddr_in client_addr{};
        auto *client_sockaddr = reinterpret_cast<sockaddr *>(&client_addr);
        socklen_t cli_addr_len = sizeof(client_addr);
        const auto pool = new BufferPool(node->log_size, node->buffer_size);
        while (RUNNING.load(std::memory_order_relaxed)) {
            // std::cout << "Still looping 2!" << std::endl;
            const auto buffer = pool->acquire();
            if (const auto size = recvfrom(server_fd, buffer, node->buffer_size, 0, client_sockaddr, &cli_addr_len); size > 0) {
                switch (const auto op = buffer[0]) {
                    case OP_PROPOSE: {
                        int proposed_slot;
                        std::memcpy(&proposed_slot, &buffer[1], sizeof(int));
                        std::memcpy(&ack_buffer[1], &proposed_slot, sizeof(int));
                        node->log[proposed_slot % node->log_size] = buffer;

                        if (sendto(server_fd, ack_buffer, 5, 0, node->peers[node->leader_id].sockaddr_ptr(), node->peers[node->leader_id].sockaddr_len()) <= 0) {
                            throw std::runtime_error("Failed to send message to node " + std::to_string(node->node_id));
                        }
                        break;
                    }

                    case OP_ACK: {
                        int acked_slot;
                        std::memcpy(&acked_slot, &buffer[1], sizeof(int));
                        node->acks[acked_slot % node->log_size] += 1;

                        const auto before_commit = node->committed.load(std::memory_order_relaxed);
                        auto current_commit = before_commit;
                        while (node->acks[current_commit % node->log_size].load() >= node->quorum) {
                            ++current_commit;
                        }

                        if (before_commit != current_commit) {
                            node->committed.store(current_commit, std::memory_order_relaxed);
                            buffer[0] = OP_COMMIT;
                            std::memcpy(&buffer[1], &current_commit, sizeof(int));
                            broadcast(server_fd, node, buffer, 5);
                        }

                        pool->release(buffer);
                        break;
                    }

                    case OP_COMMIT: {
                        int next_commit;
                        std::memcpy(&next_commit, &buffer[1], sizeof(int));
                        if (next_commit < node->committed) {
                            throw std::runtime_error("NEXT COMMIT SMALLER THAN COMMITTED");
                        }
                        node->committed.store(next_commit, std::memory_order_relaxed);
                        pool->release(buffer);
                        break;
                    }

                    default: {
                        char *ip = inet_ntoa(client_addr.sin_addr);
                        int port = ntohs(client_addr.sin_port);
                        std::cout << "Got bad op on node - " << (int) node->node_id << " from: " << ip << ":" << port << std::endl;
                        throw std::runtime_error("Invalid operation on node: " + std::to_string(node->node_id) + " op: " + std::to_string(buffer[0]) + " with size: " + std::to_string(size));
                    }
                }
            } else {
                pool->release(buffer);
            }
        }
        delete pool;
    } catch (std::exception &e) {
        std::cout << e.what() << std::endl;
    }
}

inline void node(const std::shared_ptr<Node>& node, std::vector<std::thread> &workers) {
    try {
        const auto server_fd = setup_server_socket(node->address.host(), node->address.port());
        if (node->leader_id == node->node_id) {
            workers.emplace_back([node, server_fd] { leader_commit_upward(node, server_fd); });
            workers.emplace_back([node] { leader_client_listener(node); });
        }

        workers.emplace_back([node, server_fd] { peer_listener(node, server_fd); });
    } catch (std::exception &e) {
        std::cerr << e.what() << std::endl;
    }

}

inline void broadcast(
        const int fd,
        const std::shared_ptr<Node>& node,
        const char *buffer,
        const unsigned int buffer_size
) {
    for (int i = 0; i < node->peers.size(); ++i) {
        if (i != node->node_id) {
            if (sendto(fd, buffer, buffer_size, 0, node->peers[i].sockaddr_ptr(), node->peers[i].sockaddr_len()) <= 0) {
                throw std::runtime_error("Failed to send message to node " + std::to_string(node->node_id));
            }
        }
    }
}