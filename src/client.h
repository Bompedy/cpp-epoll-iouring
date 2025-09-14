#pragma once

#include <vector>
#include <memory>
#include "shared.h"

struct ClientEntry {
    char* data;
    size_t size;
};

inline void client(
        const Address &host_address,
        const Address &leader,
        const unsigned int connections,
        const unsigned int ops,
        const unsigned int data_size,
        const double read_ratio,
        std::vector<std::thread> &workers
) {

    const auto keys = std::make_shared<std::vector<ClientEntry>>();
    const auto updateValues = std::make_shared<std::vector<ClientEntry>>();
    keys->resize(ops);
    updateValues->resize(ops);

    const std::string updateStr(data_size, 'z');

    for (unsigned int i = 0; i < ops; ++i) {
        std::string key_str = "key" + std::to_string(i);
        const auto key_buf = new char[key_str.size()];
        std::memcpy(key_buf, key_str.data(), key_str.size());
        (*keys)[i] = ClientEntry{key_buf, key_str.size()};

        const auto value_buf = new char[data_size];
        std::memset(value_buf, 'z', data_size);
        (*updateValues)[i] = ClientEntry{value_buf, data_size};
    }

    auto count = std::make_shared<std::atomic<unsigned int>>(0);
    auto read_count = std::make_shared<std::atomic<unsigned int>>(0);
    auto write_count = std::make_shared<std::atomic<unsigned int>>(0);

    const auto ops_per_conn = ops / connections;
    auto completed_connections = std::make_shared<std::atomic<int>>(0);
    auto times = std::make_shared<std::vector<long>>();
    auto write_times = std::make_shared<std::vector<long>>();
    auto read_times = std::make_shared<std::vector<long>>();
    auto start = std::make_shared<long>(0);

    times->reserve(ops);
    write_times->reserve(ops);
    read_times->reserve(ops);

    workers.emplace_back([
        completed_connections,
        connections,
        start,
        ops,
        data_size,
        count,
        read_count,
        write_count,
        times,
        read_times,
        write_times
    ] {


        try {
            while (RUNNING.load(std::memory_order_relaxed) && completed_connections->load() != connections) {
                // std::cout << "Still looping!" << std::endl;
                std::this_thread::yield();
            }
            auto end = time_millis();
            auto seconds = (float) (end - *start) / 1e3f;
            auto mbps = (((float) ops * (float) (data_size * 8)) / 1e6f) / seconds;
            auto ops_per_second = (float) ops / seconds;
            unsigned int min = 0;
            unsigned int max = 0;
            unsigned int avg = 0;
            unsigned int c = count->load(std::memory_order_relaxed);
            for (int i = 0; i < c; ++i) {
                auto time = (*times)[i];
                if (time < min) min = time;
                if (time > max) max = time;
                avg += time;
            }
            avg /= c;

            unsigned int rmin = 0;
            unsigned int rmax = 0;
            unsigned int ravg = 0;
            unsigned int rc = count->load(std::memory_order_relaxed);
            for (int i = 0; i < rc; ++i) {
                auto rtime = (*read_times)[i];
                if (rtime < rmin) rmin = rtime;
                if (rtime > rmax) rmax = rtime;
                ravg += rtime;
            }
            ravg /= rc;

            unsigned int wmin = 0;
            unsigned int wmax = 0;
            unsigned int wavg = 0;
            unsigned int wc = count->load(std::memory_order_relaxed);
            for (int i = 0; i < wc; ++i) {
                auto wtime = (*write_times)[i];
                if (wtime < wmin) wmin = wtime;
                if (wtime > wmax) wmax = wtime;
                wavg += wtime;
            }
            wavg /= wc;

            std::cout << "All - Count(" << std::endl;


            /*
                All - Count(100000) OPS(31416) Avg(300) Min(96) Max(3814) 50th(283) 90th(444) 95th(503) 99th(620) 99.9th(893) 99.99th(2245)
                Update - Count(49891) OPS(15674) Avg(353) Min(161) Max(3814) 50th(333) 90th(488) 95th(543) 99th(656) 99.9th(961) 99.99th(3688)
                Read - Count(50109) OPS(15742) Avg(246) Min(96) Max(1161) 50th(226) 90th(367) 95th(426) 99th(541) 99.9th(819) 99.99th(1083)
             */




            std::cout << "Update - Count(" << ops << ") OPS(" << ops_per_second << ") Seconds(" << seconds <<
                    ") Throughput(" << mbps << " Mbps)" << std::endl;
            std::cout << "Total reads: " << read_count->load() << std::endl;
            std::cout << "Total writes: " << write_count->load() << std::endl;
        } catch (std::exception &e) {
            std::cout << e.what() << std::endl;
        }
    });

    *start = time_millis();
    for (unsigned int i = 0; i < connections; i++) {
        workers.emplace_back([
            &leader,
            completed_connections,
            data_size,
            ops_per_conn,
            &host_address,
            i,
            read_ratio,
            count,
            write_count,
            read_count,
            times,
            write_times,
            read_times, keys, updateValues
        ] {
            try {
                auto completed_ops = 0;
                const auto client_fd = setup_server_socket(host_address.host(), host_address.port()+i);

                sockaddr_in cli_addr{};
                cli_addr.sin_family = AF_INET;
                cli_addr.sin_port = htons(leader.port());
                socklen_t addr_len = sizeof(cli_addr);
                auto *client_sockaddr = reinterpret_cast<sockaddr*>(&cli_addr);
                char write_buffer[data_size + 100];
                char read_buffer[data_size + 100];

                if (inet_pton(AF_INET, leader.host().c_str(), &cli_addr.sin_addr) <= 0) {
                    ::close(client_fd);
                    throw std::runtime_error("Invalid address");
                }

                write_buffer[0] = OP_CLIENT_REQUEST;

                bool should_send = true;
                bool was_write = false;
                long send_time = 0;
                // time_millis();

                size_t total_size = 0;
                while (RUNNING.load(std::memory_order_relaxed)) {
                    if (should_send) {
                        const unsigned int current_op = i*ops_per_conn+completed_ops;
                        send_time = time_millis();

                        const ClientEntry& key_entry = (*keys)[current_op];
                        was_write = false;
                        write_buffer[21] = REQUEST_WRITE;
                        char* ptr = &write_buffer[22];
                        std::memcpy(ptr, &key_entry.size, sizeof(unsigned int));
                        ptr += sizeof(unsigned int);
                        std::memcpy(ptr, key_entry.data, key_entry.size);
                        ptr += key_entry.size;

                        if (!isRead(read_ratio)) {
                            const ClientEntry &value_entry = (*updateValues)[current_op];
                            was_write = true;
                            write_buffer[21] = REQUEST_WRITE;

                            std::memcpy(ptr, &value_entry.size, sizeof(unsigned int));
                            ptr += sizeof(unsigned int);

                            std::memcpy(ptr, value_entry.data, value_entry.size);
                            ptr += value_entry.size;
                        } else {
                            write_buffer[21] = REQUEST_READ;
                            was_write = false;
                        }

                        total_size = ptr - &write_buffer[0];

                        // std::cout << "Writing out: " << total_size << std::endl;
                        if (sendto(client_fd, write_buffer, total_size, 0, leader.sockaddr_ptr(), leader.sockaddr_len()) <= 0) {
                            throw std::runtime_error("Failed to send message from client to leader");
                        }
                        should_send = false;
                    }
                    if (const auto size = recvfrom(client_fd, read_buffer, data_size + 100, 0, client_sockaddr, &addr_len); size > 0) {
                        if (read_buffer[0] == OP_CLIENT_RESPONSE) {
                            long recv_time = time_millis();
                            if (was_write) {
                                unsigned int index = write_count->fetch_add(1);
                                (*write_times)[index] = (recv_time - send_time);
                            } else {
                                unsigned int index = read_count->fetch_add(1);
                                (*read_times)[index] = (recv_time - send_time);
                            }
                            unsigned int index = count->fetch_add(1);
                            (*times)[index] = (recv_time - send_time);

                            ++completed_ops;
                            if (completed_ops >= ops_per_conn) {
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