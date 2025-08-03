#include <cstring>
#include <iostream>
#include <bits/ostream.tcc>
#include <fcntl.h>
#include <thread>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <vector>

std::atomic_bool active{};
std::atomic_int server_write_ops{};
std::atomic_int server_read_ops{};
std::atomic_int client_read_ops{};
std::atomic_int client_write_ops{};

struct Connection {
    char* read_buffer;
    char* write_buffer;
    size_t read_pos;
    size_t write_pos;
    bool should_write;
    bool server_side_conn;
};

uint64_t pack_fd_and_index(int fd, uint32_t index) {
    return (static_cast<uint64_t>(fd) << 32) | index;
}

void unpack_fd_and_index(uint64_t data, int &fd, uint32_t &index) {
    fd = static_cast<int>(data >> 32);
    index = static_cast<uint32_t>(data & 0xFFFFFFFF);
}

void epoll_test(const int clients_per_thread, const int threads, const int data_size, const int max_events) {
    for (int i = 0; i < threads; i++) {
        std::thread([i, clients_per_thread, max_events, data_size]() {
            auto connection_index = 0;
            std::vector<Connection> connections;
            connections.resize(clients_per_thread + 100);

            for (auto &conn: connections) {
                conn.read_buffer = new char[data_size];
                conn.write_buffer = new char[data_size];
                conn.should_write = true;
                conn.read_pos = conn.write_pos = 0;
                conn.server_side_conn = false;
            }

            const auto epoll_fd = epoll_create1(0);
            epoll_event epoll_events[max_events];

            const auto server_fd = socket(AF_INET, SOCK_STREAM, 0);
            std::cout << "server_fd: " << server_fd << std::endl;
            fcntl(server_fd, F_SETFL, O_NONBLOCK);
            constexpr int opt = 1;
            setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
            sockaddr_in addr{};
            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = INADDR_ANY;
            addr.sin_port = htons(6960 + i);

            if (bind(server_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
                perror("bind");
                return;
            }
            if (listen(server_fd, SOMAXCONN) < 0) {
                perror("listen");
                return;
            }

            epoll_event server_event{};
            server_event.events = EPOLLIN | EPOLLET;
            server_event.data.u64 = pack_fd_and_index(server_fd, 0);
            epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &server_event);
            std::cout << "Thread " << i << " listening on port " << (6960 + i) << std::endl;

            for (int c = 0; c < clients_per_thread; c++) {
                const auto client_fd = socket(AF_INET, SOCK_STREAM, 0);
                fcntl(client_fd, F_SETFL, O_NONBLOCK);
                sockaddr_in server_addr{};
                server_addr.sin_family = AF_INET;
                server_addr.sin_port = htons(6960 + i);
                server_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

                const auto result = connect(client_fd, reinterpret_cast<sockaddr *>(&server_addr), sizeof(server_addr));
                if (result == -1 && errno != EINPROGRESS && errno != EALREADY) {
                    perror("connect");
                    close(client_fd);
                    continue;
                }

                epoll_event client_event{};
                client_event.events = EPOLLIN | EPOLLOUT | EPOLLET;
                client_event.data.u64 = pack_fd_and_index(client_fd, connection_index++);
                std::cout << "My connection_index and fd: " << connection_index << " : " << client_fd << std::endl;
                epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &client_event);
            }

            while (active.load()) {
                const auto n = epoll_wait(epoll_fd, epoll_events, max_events, -1);
                for (int event_id = 0; event_id < n; event_id++) {
                    const auto event = epoll_events[event_id];
                    auto fd_and_index = epoll_events[event_id].data.u64;
                    int fd;
                    uint32_t index;
                    unpack_fd_and_index(fd_and_index, fd, index);

                    if (fd == server_fd) {
                        while (true) {
                            sockaddr_in client_addr{};
                            socklen_t addr_len = sizeof(client_addr);
                            const auto client_fd = accept(server_fd, reinterpret_cast<sockaddr *>(&client_addr), &addr_len);
                            if (client_fd < 0) {
                                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                                    break;
                                }
                                perror("accept");
                                break;
                            }

                            fcntl(client_fd, F_SETFL, O_NONBLOCK);

                            epoll_event client_event{};
                            client_event.events = EPOLLIN | EPOLLET | EPOLLOUT;
                            connections[connection_index].server_side_conn = true;
                            client_event.data.u64 = pack_fd_and_index(client_fd, connection_index++);
                            epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &client_event);
                        }
                    } else {
                        auto& conn = connections[index];

                        if (event.events & EPOLLERR || event.events & EPOLLHUP || event.events & EPOLLRDHUP) {
                            std::cout << "Closing fd!" << std::endl;
                            close(fd);
                        }

                        if (event.events & EPOLLIN) {
                            while (true) {
                                const auto bytes_read = read(fd, conn.read_buffer + conn.read_pos, data_size - conn.read_pos);
                                if (bytes_read == -1) {
                                    if (errno == EAGAIN || errno == EWOULDBLOCK) {
                                        break;
                                    }
                                    perror("read");
                                    close(fd);
                                    break;
                                }
                                if (bytes_read == 0) {
                                    close(fd);
                                    break;
                                }

                                conn.read_pos += bytes_read;
                                if (conn.read_pos == data_size) {
                                    conn.read_pos = 0;
                                    conn.write_pos = 0;
                                    conn.should_write = true;

                                    if (conn.server_side_conn) {
                                        server_read_ops += 1;
                                    } else {
                                        client_read_ops += 1;
                                    }
                                }
                            }
                        }

                        if (event.events & EPOLLOUT) {
                            if (conn.should_write) {
                                while (conn.write_pos < data_size) {
                                    const auto bytes_written = write(fd, conn.write_buffer + conn.write_pos,data_size - conn.write_pos);
                                    if (bytes_written == -1) {
                                        if (errno == EAGAIN || errno == EWOULDBLOCK) {
                                            break;
                                        }
                                        perror("write");
                                        close(fd);
                                        break;
                                    }
                                    conn.write_pos += bytes_written;
                                }

                                if (conn.write_pos == data_size) {
                                    conn.write_pos = 0;
                                    conn.should_write = false;

                                    if (conn.server_side_conn) {
                                        server_write_ops += 1;
                                    } else {
                                        client_write_ops += 1;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }).detach();
    }
}

int main() {
    // active.store(true);
    // epoll_test(1, 10, 4, 64);

    while (true) {

    }

    std::thread monitor_thread([]() {
        int64_t lastClientRead = 0;
        int64_t lastClientWrite = 0;
        int64_t lastServerRead = 0;
        int64_t lastServerWrite = 0;

        while (active.load()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));

            int64_t cr = client_read_ops.load();
            int64_t cw = client_write_ops.load();
            int64_t sr = server_read_ops.load();
            int64_t sw = server_write_ops.load();

            std::cout << "Client ops: read=" << (cr - lastClientRead)
                    << " write=" << (cw - lastClientWrite) << std::endl;
            std::cout << "Server ops: read=" << (sr - lastServerRead)
                    << " write=" << (sw - lastServerWrite) << std::endl;

            lastClientRead = cr;
            lastClientWrite = cw;
            lastServerRead = sr;
            lastServerWrite = sw;
        }
    });

    monitor_thread.join();

    return 0;
}
