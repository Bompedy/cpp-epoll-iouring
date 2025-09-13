#pragma once

#include <vector>
#include <atomic>
#include <thread>
#include <unordered_map>

#include "shared.h"

struct Node;

inline void broadcast(int fd,const Node &node, const char *buffer, unsigned int buffer_size);

struct Node {
    const unsigned char node_id;
    const unsigned char leader_id;
    std::vector<Address> peers;
    const int buffer_size;
    const int log_size;
    const int quorum;
    const Address &address;

    char **log;
    std::atomic<unsigned char> *acks;
    std::atomic<unsigned int> committed{};

    Node(
            const unsigned char id,
            const unsigned char leader_id,
            const std::vector<Address> &peers,
            const int buffer_size,
            const int log_size
    ): node_id(id), leader_id(leader_id), peers(peers), buffer_size(buffer_size), log_size(log_size),
        quorum((int) peers.size() / 2 + 1), address(peers[id])
    {
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

inline void leader_commit_upward(const Node &node, const int server_fd) {
    try {
        auto pool = new BufferPool(node.log_size, node.buffer_size);
        char** storage = new char*[node.log_size];
        // std::unordered_map<std::string, char*> storage{};

        unsigned int consumed = 0;
        auto temp_buffer = new char[100000];
        while (RUNNING.load(std::memory_order_relaxed)) {
            if (node.committed.load(std::memory_order_relaxed) > consumed) {
                const auto data = node.log[consumed % node.log_size];
                if (data == nullptr) {
                    throw std::runtime_error("node commit failed data was null");
                }
                const auto is_read = data[21] == REQUEST_READ;
                const auto is_write = data[21] == REQUEST_WRITE;

                if (is_write) {
                    unsigned int key;
                    std::memcpy(&key, &data[22], sizeof(unsigned int));

                    unsigned int value_size;
                    std::memcpy(&key, &data[26], sizeof(unsigned int));

                    const auto write_buffer = pool->acquire();

                    storage[key] = write_buffer;

                    // fill kv
                }
                sockaddr sender_addr{};
                std::memcpy(&sender_addr, &data[5], sizeof(sockaddr_in));

                if (is_write) {
                    temp_buffer[0] = OP_CLIENT_RESPONSE;

                    if (sendto(server_fd, temp_buffer, 1, 0, &sender_addr, sizeof(sockaddr)) <= 0) {
                        throw std::runtime_error(
                            "Failed to send message to node " + std::to_string(node.node_id));
                    }
                } else if (is_read) {
                    std::cout << "Somehow its a read?" << std::endl;
                } else throw std::invalid_argument("Invalid leader address format");
                consumed++;
            } else std::this_thread::yield();
        }
    } catch (std::exception &e) {
        std::cout << e.what() << std::endl;
    }
}

inline void leader_client_listener(const Node &node) {
    try {
        const auto request_fd = setup_server_socket("127.0.0.1", 7069);
        unsigned int slot = 0;
        auto pool = new BufferPool(node.log_size, node.buffer_size);
        std::cout << "Allocated pool for node: " << node.node_id << std::endl;
        sockaddr_in client_addr{};
        auto *client_sockaddr = reinterpret_cast<sockaddr *>(&client_addr);
        socklen_t cli_addr_len = sizeof(client_addr);
        while (RUNNING.load(std::memory_order_relaxed)) {
            // std::cout << "Still looping 3!" << std::endl;
            const auto buffer = pool->acquire();
            cli_addr_len = sizeof(client_addr);
            if (const auto size = recvfrom(request_fd, buffer, node.buffer_size, 0, client_sockaddr, &cli_addr_len); size > 0) {
                if (buffer[0] == OP_CLIENT_REQUEST) {
                    const auto next_slot = slot++;
                    if (node.acks[next_slot % node.log_size] != 0) {
                        throw std::runtime_error(
                                "OUT OF LOG SPACE AT INDEX: " + std::to_string(next_slot) + " " +
                                std::to_string(node.acks[next_slot % node.log_size]));
                    }
                    node.acks[next_slot % node.log_size].store(1);
                    std::memcpy(&buffer[1], &next_slot, sizeof(int));
                    std::memcpy(&buffer[5], &client_addr, cli_addr_len);
                    node.log[next_slot % node.log_size] = buffer;
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
        close(request_fd);
    } catch (std::exception &e) {
        std::cout << e.what() << std::endl;
    }
}

inline void peer_listener(Node &node, const int server_fd) {
    try {
        char ack_buffer[5];
        ack_buffer[0] = OP_ACK;
        sockaddr_in client_addr{};
        auto *client_sockaddr = reinterpret_cast<sockaddr *>(&client_addr);
        socklen_t cli_addr_len = sizeof(client_addr);
        const auto pool = new BufferPool(node.log_size, node.buffer_size);
        std::cout << "Created peer listener log: " << node.node_id << std::endl;
        while (RUNNING.load(std::memory_order_relaxed)) {
            // std::cout << "Still looping 2!" << std::endl;
            const auto buffer = pool->acquire();
            if (const auto size = recvfrom(server_fd, buffer, node.buffer_size, 0, client_sockaddr, &cli_addr_len); size > 0) {
                switch (const auto op = buffer[0]) {
                    case OP_PROPOSE: {
                        int proposed_slot;
                        std::memcpy(&proposed_slot, &buffer[1], sizeof(int));
                        std::memcpy(&ack_buffer[1], &proposed_slot, sizeof(int));
                        node.log[proposed_slot % node.log_size] = buffer;

                        if (sendto(server_fd, ack_buffer, 5, 0, node.peers[node.leader_id].sockaddr_ptr(), node.peers[node.leader_id].sockaddr_len()) <= 0) {
                            throw std::runtime_error("Failed to send message to node " + std::to_string(node.node_id));
                        }
                        break;
                    }

                    case OP_ACK: {
                        int acked_slot;
                        std::memcpy(&acked_slot, &buffer[1], sizeof(int));
                        node.acks[acked_slot % node.log_size] += 1;

                        const auto before_commit = node.committed.load(std::memory_order_relaxed);
                        auto current_commit = before_commit;
                        while (node.acks[current_commit % node.log_size].load() >= node.quorum) {
                            ++current_commit;
                        }

                        if (before_commit != current_commit) {
                            node.committed.store(current_commit, std::memory_order_relaxed);
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
                        if (next_commit < node.committed) {
                            throw std::runtime_error("NEXT COMMIT SMALLER THAN COMMITTED");
                        }
                        node.committed.store(next_commit, std::memory_order_relaxed);
                        pool->release(buffer);
                        break;
                    }

                    default: {
                        char *ip = inet_ntoa(client_addr.sin_addr);
                        int port = ntohs(client_addr.sin_port);
                        std::cout << "Got bad op on node - " << (int) node.node_id << " from: " << ip << ":" << port << std::endl;
                        throw std::runtime_error("Invalid operation on node: " + std::to_string(node.node_id) + " op: " + std::to_string(buffer[0]) + " with size: " + std::to_string(size));
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

inline void node(Node &node, std::vector<std::thread> &workers) {
    try {
        const auto server_fd = setup_server_socket(node.address.host(), node.address.port());
        if (node.leader_id == node.node_id) {
            workers.emplace_back([&node, server_fd] { leader_commit_upward(node, server_fd); });
            workers.emplace_back([&node] { leader_client_listener(node); });
        }

        workers.emplace_back([&node, server_fd] { peer_listener(node, server_fd); });
    } catch (std::exception &e) {
        std::cerr << e.what() << std::endl;
    }

}

inline void broadcast(
        const int fd,
        const Node &node,
        const char *buffer,
        const unsigned int buffer_size
) {
    for (int i = 0; i < node.peers.size(); ++i) {
        if (i != node.node_id) {
            if (sendto(fd, buffer, buffer_size, 0, node.peers[i].sockaddr_ptr(), node.peers[i].sockaddr_len()) <= 0) {
                throw std::runtime_error("Failed to send message to node " + std::to_string(node.node_id));
            }
        }
    }
}