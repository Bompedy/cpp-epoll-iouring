#include <csignal>
#include <cstring>
#include <iostream>
#include <bits/ostream.tcc>
#include <fcntl.h>
#include <thread>
#include <unordered_map>
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

uint64_t pack_fd_and_index(const int fd, const uint32_t index) {
    return (static_cast<uint64_t>(fd) << 32) | index;
}

void unpack_fd_and_index(const uint64_t data, int &fd, uint32_t &index) {
    fd = static_cast<int>(data >> 32);
    index = static_cast<uint32_t>(data & 0xFFFFFFFF);
}

std::unordered_map<std::string, std::string> parse_flags(int argc, char* argv[]) {
    std::unordered_map<std::string, std::string> flags;

    for (int i = 1; i < argc; ++i) {
        if (std::string arg = argv[i]; arg.rfind("--", 0) == 0) {
            const auto eq_pos = arg.find('=');
            if (eq_pos != std::string::npos) {
                const auto key = arg.substr(2, eq_pos - 2);
                const auto value = arg.substr(eq_pos + 1);
                flags[key] = value;
            } else {
                std::cerr << "Invalid flag format: " << arg << std::endl;
            }
        }
    }

    return flags;
}

void epoll_test(
    const bool is_client,
    const int clients_per_thread,
    const int threads,
    const int data_size,
    const int max_events,
    const std::string& ip_address,
    const int base_port
) {
    for (int i = 0; i < threads; i++) {
        std::thread([i, clients_per_thread, max_events, data_size, base_port, ip_address, is_client]() {
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

            int server_fd = 0;

            if (!is_client) {
                server_fd = socket(AF_INET, SOCK_STREAM, 0);
                std::cout << "server_fd: " << server_fd << std::endl;
                fcntl(server_fd, F_SETFL, O_NONBLOCK);
                constexpr int opt = 1;
                setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
                sockaddr_in addr{};
                addr.sin_family = AF_INET;
                inet_pton(AF_INET, ip_address.c_str(), &addr.sin_addr);
                addr.sin_port = htons(base_port + i);

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
                std::cout << "Thread " << i << " listening on port " << (base_port + i) << std::endl;
            }

            if (is_client) {
                for (int c = 0; c < clients_per_thread; c++) {
                    const auto client_fd = socket(AF_INET, SOCK_STREAM, 0);
                    fcntl(client_fd, F_SETFL, O_NONBLOCK);
                    sockaddr_in server_addr{};
                    server_addr.sin_family = AF_INET;
                    server_addr.sin_port = htons(base_port + i);
                    inet_pton(AF_INET, ip_address.c_str(), &server_addr.sin_addr);

                    const auto result = connect(client_fd, reinterpret_cast<sockaddr *>(&server_addr),
                                                sizeof(server_addr));
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
            }

            while (active.load()) {
                const auto n = epoll_wait(epoll_fd, epoll_events, max_events, -1);
                for (int event_id = 0; event_id < n; event_id++) {
                    const auto event = epoll_events[event_id];
                    auto fd_and_index = epoll_events[event_id].data.u64;
                    int fd;
                    uint32_t index;
                    unpack_fd_and_index(fd_and_index, fd, index);

                    if (!is_client && fd == server_fd) {
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

                                    if (is_client) client_read_ops.fetch_add(1); else server_read_ops.fetch_add(1);
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
                                    if (is_client) {
                                        client_write_ops.fetch_add(1);
                                    } else server_write_ops.fetch_add(1);
                                }
                            }
                        }
                    }
                }
            }
        }).detach();
    }
}

int main(int argc, char *argv[]) {
    auto flags = parse_flags(argc, argv);

    if (!flags.contains("clients") || !flags.contains("threads") || !flags.contains("data") ||
        !flags.contains("events") || !flags.contains("host") || !flags.contains("port") || !flags.contains("client")) {
        std::cerr << "Usage:\n"
                << "  --client=0|1\n"
                << "  --clients=N\n"
                << "  --threads=N\n"
                << "  --data=N\n"
                << "  --events=N\n"
                << "  --host=IP_ADDRESS\n"
                << "  --port=BASE_PORT\n";
        return 1;
    }

    const bool is_client = flags["client"] == "1";
    const int clients_per_thread = std::stoi(flags["clients"]);
    const int threads = std::stoi(flags["threads"]);
    const int data_size = std::stoi(flags["data"]);
    const int max_events = std::stoi(flags["events"]);
    const std::string ip_address = flags["host"];
    const int base_port = std::stoi(flags["port"]);
    std::signal(SIGINT, [](int) { active.store(false); });


    active.store(true);
    epoll_test(is_client, clients_per_thread, threads, data_size, max_events, ip_address, base_port);

    std::thread monitor_thread([is_client]() {
        auto lastClientRead = 0;
        auto lastClientWrite = 0;
        auto lastServerRead = 0;
        auto lastServerWrite = 0;

        while (active.load()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));

            const auto cr = client_read_ops.load();
            const auto cw = client_write_ops.load();
            const auto sr = server_read_ops.load();
            const auto sw = server_write_ops.load();

            if (is_client) {
                std::cout << "Client ops: read=" << (cr - lastClientRead) << " write=" << (cw - lastClientWrite) << std::endl;
            } else {
                std::cout << "Server ops: read=" << (sr - lastServerRead) << " write=" << (sw - lastServerWrite) << std::endl;
            }


            lastClientRead = cr;
            lastClientWrite = cw;
            lastServerRead = sr;
            lastServerWrite = sw;
        }
    });

    monitor_thread.join();

    return 0;
}
