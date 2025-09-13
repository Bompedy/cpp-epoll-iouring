#pragma once

#include <vector>
#include <memory>
#include "shared.h"

inline void client(
        const Address &leader,
        const unsigned int connections,
        const unsigned int ops,
        const unsigned int data_size,
        const unsigned short start_port,
        std::vector<std::thread> &workers
) {
    const auto ops_per_conn = ops / connections;
    auto completed_connections = std::make_shared<std::atomic<int>>(0);
    auto send_times = std::make_shared<std::vector<long> >();
    auto start = std::make_shared<long>(0);
    workers.emplace_back([completed_connections, connections, start, ops, data_size]() {
        try {
            while (RUNNING.load(std::memory_order_relaxed) && completed_connections->load() != connections) {
                // std::cout << "Still looping!" << std::endl;
                std::this_thread::yield();
            }
            auto end = time_millis();
            auto seconds = (float) (end - *start) / 1e3f;
            auto mbps = (((float) ops * (float) (data_size * 8)) / 1e6f) / seconds;
            auto ops_per_second = (float) ops / seconds;
            std::cout << "Update - Count(" << ops << ") OPS(" << ops_per_second << ") Seconds(" << seconds <<
                    ") Throughput(" << mbps << " Mbps)" << std::endl;
        } catch (std::exception &e) {
            std::cout << e.what() << std::endl;
        }
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    *start = time_millis();
    for (unsigned int i = 0; i < connections; i++) {
        workers.emplace_back([&leader, completed_connections, data_size, ops_per_conn, start_port, i]() {
            try {
                auto completed_ops = 0;
                const auto client_fd = setup_server_socket("127.0.0.1", start_port+i);

                sockaddr_in cli_addr{};
                cli_addr.sin_family = AF_INET;
                cli_addr.sin_port = htons(leader.port());
                socklen_t addr_len = sizeof(cli_addr);
                auto *client_sockaddr = reinterpret_cast<sockaddr *>(&cli_addr);
                char write_buffer[data_size + 100];
                char read_buffer[data_size + 100];

                if (inet_pton(AF_INET, leader.host().c_str(), &cli_addr.sin_addr) <= 0) {
                    close(client_fd);
                    throw std::runtime_error("Invalid address");
                }

                write_buffer[0] = OP_CLIENT_REQUEST;

                bool should_send = true;
                bool is_write = true;

                while (RUNNING.load(std::memory_order_relaxed)) {
                    // std::cout << "Still looping 1!" << std::endl;
                    if (should_send) {
                        if (is_write) {
                            write_buffer[21] = REQUEST_WRITE;
                            //
                            unsigned int key_size;
                            memcpy(&key_size, write_buffer + 22, sizeof(unsigned int));
                            char *key =
                            // key, value
                        } else {
                            write_buffer[21] = REQUEST_READ;
                            // key
                        }
                        // std::cout << "Sending out client request" << std::endl;
                        if (sendto(client_fd, write_buffer, data_size + 22, 0, leader.sockaddr_ptr(),
                                   leader.sockaddr_len()) <= 0) {
                            throw std::runtime_error("Failed to send message from client to leader");
                        }
                        should_send = false;
                    }
                    if (const auto size = recvfrom(client_fd, read_buffer, data_size + 100, 0, client_sockaddr, &addr_len); size > 0) {
                        if (read_buffer[0] == OP_CLIENT_RESPONSE) {
                            // std::cout << "Got client response" << std::endl;
                            ++completed_ops;
                            // if (completed_ops % 10000 == 0) {
                            //     // std::cout << completed_ops << std::endl;
                            // }
                            if (completed_ops >= ops_per_conn) {
                                std::cout << "Incrementing completed: " << completed_ops << std::endl;
                                completed_connections->fetch_add(1);
                                break;
                            }
                            should_send = true;
                        } else {
                            throw std::runtime_error("Invalid `client response");
                        }
                    }
                }
            } catch (std::exception &e) {
                std::cout << e.what() << std::endl;
            }
        });
    }
}