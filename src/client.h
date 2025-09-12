#pragma once

#include <vector>
#include <memory>
#include "shared.h"

void client(
        const Address &leader,
        const unsigned int connections,
        const unsigned int ops,
        const unsigned int data_size,
        std::vector<std::thread> &workers,
        std::atomic<bool> &running
) {
    workers.emplace_back([connections, ops, data_size, &leader, &running]() {
        const auto ops_per_conn = ops / connections;
        auto completed_connections = std::make_shared<std::atomic<int>>(0);
        auto send_times = std::make_shared<std::vector<long> >();
        auto start = std::make_shared<long>(0);
        std::thread completion_thread([completed_connections, connections, ops, data_size, start]() {
            while (completed_connections->load() != connections) {
                std::this_thread::yield();
            }
            auto end = time_millis();
            auto seconds = (float) (end - *start) / 1e3f;
            auto mbps = (((float) ops * (float) (data_size * 8)) / 1e6f) / seconds;
            auto ops_per_second = (float) ops / seconds;
            std::cout << "Update - Count(" << ops << ") OPS(" << ops_per_second << ") Seconds(" << seconds <<
                      ") Throughput(" << mbps << " Mbps)" << std::endl;
        });
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));

        std::thread connection_threads[connections];
        *start = time_millis();
        for (unsigned int i = 0; i < connections; i++) {
            connection_threads[i] = std::thread([&leader, completed_connections, data_size, ops_per_conn, &running]() {
                // std::cout << "Starting thread" << std::endl;
                auto completed_ops = 0;
                const auto client_fd = socket(AF_INET, SOCK_DGRAM, 0);
                if (!tune_socket(client_fd, 1024 * 4 * 1024)) {
                    throw std::runtime_error("Failed to create socket");
                }

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

                while (running.load(std::memory_order_relaxed)) {
                    if (should_send) {
                        if (is_write) {
                            write_buffer[21] = REQUEST_WRITE;
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
                    if (const auto size = recvfrom(client_fd, read_buffer, data_size + 100, 0, client_sockaddr, &addr_len);
                            size > 0) {
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
                            throw std::runtime_error("Invalid client response");
                        }
                    }
                }
            });
        }
        completion_thread.join();
        for (unsigned int i = 0; i < connections; i++) {
            connection_threads[i].join();
        }
    });
}