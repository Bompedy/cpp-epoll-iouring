#pragma once

#include <vector>
#include <atomic>
#include <thread>
#include "shared.h"

struct Node;
int setup_server_socket(const std::string &address, unsigned short port);
inline void broadcast(int fd,const Node &node, const char *buffer,unsigned int buffer_size);

struct Node {
    const unsigned char node_id;
    const bool is_leader;
    const std::vector<Address> peers;
    const int buffer_size;
    const int log_size;
    const int quorum;
    const Address &address;

    char **log;
    std::atomic<unsigned char> *acks;
    std::atomic<unsigned int> committed{};

    Node(
            const unsigned char id,
            const bool is_leader,
            const std::vector<Address> &peers,
            const int buffer_size,
            const int log_size
    ): node_id(id), is_leader(is_leader), peers(peers), buffer_size(buffer_size), log_size(log_size),
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

void leader_commit_upward(Node &node, std::atomic<bool> &running, const int server_fd) {
    long last_print = 0;
    unsigned int consumed = 0;
    while (running.load(std::memory_order_relaxed)) {
        long time = time_millis();
        if (time - last_print > 500) {
            printf("Committed(%d) Consumed(%d)\n", node.committed.load(std::memory_order_relaxed), consumed);
            last_print = time;
        }
        if (node.committed.load(std::memory_order_relaxed) > consumed) {
            const auto data = node.log[consumed % node.log_size];
            if (data == nullptr) {
                if (node.is_leader) {
                    throw std::runtime_error("such a bad problem");
                }

                // break;
                consumed++;
                // std::cout << "Stuck trying to consume slot: " << consumed << std::endl;
                continue;
            }
            const auto is_read = data[21] == REQUEST_READ;
            const auto is_write = data[21] == REQUEST_WRITE;

            if (is_write) {
                // fill kv
            }
            if (node.is_leader) {
                sockaddr sender_addr{};
                std::memcpy(&sender_addr, &data[5], sizeof(sockaddr_in));
                auto* sender_in = reinterpret_cast<sockaddr_in*>(&sender_addr);
                char *ip = inet_ntoa(sender_in->sin_addr);
                int port = ntohs(sender_in->sin_port);
                // std::cout << "Sending response to client at " << ip << ":" << port << std::endl;
                if (is_write) {
                    // std::cout << "Gonna write back to client for slot: " << consumed << std::endl;
                    data[0] = OP_CLIENT_RESPONSE;
                    if (sendto(server_fd, data, 1, 0, &sender_addr, sizeof(sockaddr)) <= 0) {
                        throw std::runtime_error(
                                "Failed to send message to node " + std::to_string(node.node_id));
                    }
                } else if (is_read) {
                } else throw std::invalid_argument("Invalid leader address format");
            }
            consumed++;
        } else std::this_thread::yield();
    }
}

void leader_client_listener(Node &node, std::atomic<bool> &running, const int server_fd) {
    const auto request_fd = setup_server_socket("127.0.0.1", 7069);
    unsigned int slot = 0;
    auto pool = new BufferPool(node.log_size, node.buffer_size);
    sockaddr_in client_addr{};
    auto *client_sockaddr = reinterpret_cast<sockaddr *>(&client_addr);
    socklen_t cli_addr_len = sizeof(client_addr);
    while (running.load(std::memory_order_relaxed)) {
        const auto buffer = pool->acquire();
        cli_addr_len = sizeof(client_addr);
        if (const auto size = recvfrom(request_fd, buffer, node.buffer_size, 0, client_sockaddr,
                                       &cli_addr_len); size > 0) {
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
                broadcast(server_fd, node, buffer, size);
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
}

void peer_listener(Node &node, std::atomic<bool> &running, const int server_fd) {
    char ack_buffer[5];
    ack_buffer[0] = OP_ACK;
    sockaddr_in client_addr{};
    auto *client_sockaddr = reinterpret_cast<sockaddr *>(&client_addr);
    socklen_t cli_addr_len = sizeof(client_addr);
    const auto pool = new BufferPool(node.log_size * 2, node.buffer_size);
    while (running.load(std::memory_order_relaxed)) {
        const auto buffer = pool->acquire();
        if (const auto size = recvfrom(server_fd, buffer, node.buffer_size, 0, client_sockaddr, &cli_addr_len);
                size > 0) {
            switch (const auto op = buffer[0]) {
                case OP_PROPOSE: {
                    int proposed_slot;
                    std::memcpy(&proposed_slot, &buffer[1], sizeof(int));
                    std::memcpy(&ack_buffer[1], &proposed_slot, sizeof(int));
                    node.log[proposed_slot % node.log_size] = buffer;

                    if (sendto(server_fd, ack_buffer, 5, 0, client_sockaddr,
                               cli_addr_len) <= 0) {
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
                    throw std::runtime_error("Invalid operation on node: " + std::to_string(node.node_id) + " op: " + std::to_string(buffer[0]));
                }
            }
        } else {
            pool->release(buffer);
        }
    }
    delete pool;
}

void node(Node &node, std::vector<std::thread> &workers, std::atomic<bool> &running) {
    workers.emplace_back([&node, &running]() {
        try {
            const auto server_fd = setup_server_socket(node.address.host(), node.address.port());
            std::thread commit_upward_thread;
            std::thread client_listener_thread;
            if (node.is_leader) {
                commit_upward_thread = std::thread(leader_commit_upward, std::ref(node), std::ref(running), server_fd);
                client_listener_thread = std::thread(leader_client_listener, std::ref(node), std::ref(running), server_fd);
            }
            std::thread peer_listener_thread = std::thread(peer_listener, std::ref(node), std::ref(running), server_fd);
            if (commit_upward_thread.joinable()) commit_upward_thread.join();
            if (client_listener_thread.joinable()) client_listener_thread.join();
            peer_listener_thread.join();
            close(server_fd);
        } catch (std::exception &e) {
            std::cerr << e.what() << std::endl;
        }
    });
}

int setup_server_socket(const std::string &address, const unsigned short port) {
    constexpr int opt = 1;
    const auto server_fd = socket(AF_INET, SOCK_DGRAM, 0);

    if (server_fd < 0) {
        throw std::runtime_error("server socket failed: " + std::string(std::strerror(errno)));
    }

    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt)) < 0) {
        close(server_fd);
        throw std::runtime_error("Failed to set reuseport to socket");
    }

    const auto flags = fcntl(server_fd, F_GETFL, 0);
    if (flags == -1) {
        perror("fcntl F_GETFL");
        return false;
    }

    if (!tune_socket(server_fd, 1024 * 4 * 1024)) {
        close(server_fd);
        throw std::runtime_error("Failed to create tune socket");
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

    if (inet_pton(AF_INET, address.c_str(), &addr.sin_addr) <= 0) {
        close(server_fd);
        throw std::runtime_error("Invalid address: " + address);
    }

    if (bind(server_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
        close(server_fd);
        throw std::runtime_error("Failed to bind to port " + std::to_string(port));
    }

    return server_fd;
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