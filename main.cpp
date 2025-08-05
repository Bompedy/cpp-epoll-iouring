#include <csignal>
#include <cstring>
#include <iostream>
#include <bits/ostream.tcc>
#include <fcntl.h>
#include <functional>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <vector>
#include "io_uring.h"
#include <sys/mman.h>
#include <netinet/tcp.h>

int io_uring_setup(const unsigned entries, io_uring_params *params) {
    const int ring_fd = syscall(__NR_io_uring_setup, entries, params);
    if (ring_fd < 0) {
        throw std::runtime_error("io_uring_setup failed: " + std::string(std::strerror(errno)));
    }
    return ring_fd;
}

int io_uring_enter(
    const int ring_fd,
    const unsigned int to_submit,
    const unsigned int min_complete,
    const unsigned int flags
) {
    const int result = syscall(__NR_io_uring_enter, ring_fd, to_submit, min_complete, flags, nullptr, 0);
    if (result < 0) {
        throw std::runtime_error("io_uring_enter failed: " + std::string(std::strerror(errno)));
    }
    return result;
}

int io_uring_register(unsigned int ring_fd, unsigned int op, void *arg, unsigned int nr_args) {
    const int result = syscall(__NR_io_uring_register, ring_fd, op, arg, nr_args);
    if (result < 0) {
        throw std::runtime_error("io_uring_register failed: " + std::string(std::strerror(errno)));
    }
    return result;
}
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

uint64_t pack_fd_index_opcode(int fd, uint32_t index, uint8_t opcode) {
    return (static_cast<uint64_t>(fd) << 32) |
           ((static_cast<uint64_t>(index) & 0xFFFFFF) << 8) |
           (opcode & 0xFF);
}

void unpack_fd_index_opcode(uint64_t data, int &fd, uint32_t &index, uint8_t &opcode) {
    fd = static_cast<int>(data >> 32);
    index = static_cast<uint32_t>((data >> 8) & 0xFFFFFF);
    opcode = static_cast<uint8_t>(data & 0xFF);
}

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

std::vector<int> split_ports(const std::string& port_by_thread) {
    std::vector<int> ports;
    std::stringstream ss(port_by_thread);
    std::string port_str;

    while (std::getline(ss, port_str, ',')) {
        ports.push_back(std::stoi(port_str));
    }

    return ports;
}

void pin_thread_to_core(int core_id) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);

    const auto thread = pthread_self();
    if (pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset) != 0) {
        perror("pthread_setaffinity_np");
    }
}

void epoll_test(
    const bool is_client,
    const int clients_per_thread,
    const int threads,
    const int data_size,
    const int max_events,
    const std::string& ip_address,
    const std::vector<int>& ports,
    const bool ports_count_up
) {
    for (int i = 0; i < threads; i++) {
        const auto port = ports_count_up ? (ports[0] + i) : ports[i % ports.size()];
        std::thread([i, clients_per_thread, max_events, data_size, port, ip_address, is_client]() {
            const int core_id = i % std::thread::hardware_concurrency();
            pin_thread_to_core(core_id);
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
                addr.sin_port = htons(port);

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
                std::cout << "Thread " << i << " listening on port " << (port) << std::endl;
            } else  {
                for (int c = 0; c < clients_per_thread; c++) {
                    const auto client_fd = socket(AF_INET, SOCK_STREAM, 0);
                    fcntl(client_fd, F_SETFL, O_NONBLOCK);
                    sockaddr_in server_addr{};
                    server_addr.sin_family = AF_INET;
                    server_addr.sin_port = htons(port);
                    inet_pton(AF_INET, ip_address.c_str(), &server_addr.sin_addr);

                    const auto result = connect(client_fd, reinterpret_cast<sockaddr *>(&server_addr), sizeof(server_addr));
                    if (result == -1 && errno != EINPROGRESS && errno != EALREADY) {
                        perror("connect");
                        close(client_fd);
                        continue;
                    }

                    int flag = 1;
                    if (setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag)) < 0) {
                        perror("setsockopt TCP_NODELAY failed");
                    }

                    epoll_event client_event{};
                    client_event.events = EPOLLIN | EPOLLOUT | EPOLLET;
                    client_event.data.u64 = pack_fd_and_index(client_fd, connection_index++);
                    std::cout << "My connection_index and fd: " << connection_index << " : " << client_fd << std::endl;
                    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &client_event);
                }
            }

            while (active.load()) {
                const auto n = epoll_wait(epoll_fd, epoll_events, max_events, 0);
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

                            int flag = 1;
                            if (setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag)) < 0) {
                                perror("setsockopt TCP_NODELAY failed");
                            }


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

// void iouring_test(
//     const bool is_client,
//     const int clients_per_thread,
//     const int threads,
//     const int data_size,
//     const int max_events,
//     const std::string &ip_address,
//     const int base_port
// ) {
//     for (int threadId = 0; threadId < threads; threadId++) {
//         std::thread([threadId, clients_per_thread, max_events, data_size, base_port, ip_address, is_client]() {
//             try {
//                 int connection_index = 0;
//                 constexpr auto params = new io_uring_params();
//                 params->flags |= IORING_SETUP_SQPOLL | IORING_SETUP_SINGLE_ISSUER;
//                 const auto ring_fd = io_uring_setup(1000, params);
//                 const auto sq_ring_size = params->sq_off.array + params->sq_entries + sizeof(__u32);
//                 const auto sq_ptr = mmap(nullptr, sq_ring_size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ring_fd, IORING_OFF_SQ_RING);
//                 if (sq_ptr == MAP_FAILED) throw std::runtime_error("mmap failed on sq_ptr");
//                 const auto sqes_size = params->sq_entries * sizeof(io_uring_sqe);
//                 const auto sqes =
//                         static_cast<struct io_uring_sqe *>(
//                             mmap(nullptr, sqes_size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ring_fd,
//                                  IORING_OFF_SQES)
//                         );
//                 if (sqes == MAP_FAILED) throw std::runtime_error("mmap failed on sqes");
//                 const auto cq_ring_size = params->cq_off.cqes + params->cq_entries * sizeof(struct io_uring_cqe);
//                 const auto cq_ptr = mmap(NULL, cq_ring_size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ring_fd, IORING_OFF_CQ_RING);
//                 if (cq_ptr == MAP_FAILED) throw std::runtime_error("mmap failed on cqes");
//
//                 const auto cq_head = reinterpret_cast<std::atomic<uint32_t> *>(static_cast<char *>(cq_ptr) + params->cq_off.head);
//                 const auto cq_tail = reinterpret_cast<std::atomic<uint32_t> *>(static_cast<char *>(cq_ptr) + params->cq_off.tail);
//                 const auto cq_ring_mask = reinterpret_cast<uint32_t *>(static_cast<char *>(cq_ptr) + params->cq_off.ring_mask);
//                 const auto *cqes = reinterpret_cast<struct io_uring_cqe *>(static_cast<char *>(cq_ptr) + params->cq_off.cqes);
//
//                 const auto sq_head = reinterpret_cast<std::atomic<uint32_t>*>(static_cast<char*>(sq_ptr) + params->sq_off.head);
//                 const auto sq_tail = reinterpret_cast<std::atomic<uint32_t>*>(static_cast<char*>(sq_ptr) + params->sq_off.tail);
//                 const auto sq_ring_mask = reinterpret_cast<uint32_t*>(static_cast<char*>(sq_ptr) + params->sq_off.ring_mask);
//                 const auto sq_array = reinterpret_cast<uint32_t *>(static_cast<char *>(sq_ptr) + params->sq_off.array);
//                 const auto sq_flags = reinterpret_cast<std::atomic<uint32_t> *>(static_cast<char *>(sq_ptr) + params->sq_off.flags);
//
//                 auto to_submit = 0;
//
//                 auto submit = [sq_head, sq_tail, sq_ring_mask, sqes, &to_submit](const std::function<void(io_uring_sqe&)> &callback) {
//                     const auto head = sq_head->load();
//                     const auto tail = sq_tail->load();
//                     const auto used = tail - head;
//                     const auto space = params->sq_entries - used;
//                     if (space <= 0) throw std::runtime_error("out of space in submission queue!");
//                     const auto index = tail & *sq_ring_mask;
//                     io_uring_sqe* entry = &sqes[index];
//                     memset(entry, 0, sizeof(*entry));
//                     callback(*entry);
//                     to_submit += 1;
//                     sq_tail->store(tail + 1, std::memory_order_seq_cst);
//                 };
//
//
//                 sockaddr_in server_addr{};
//                 server_addr.sin_family = AF_INET;
//                 server_addr.sin_port = htons(base_port + threadId);
//                 inet_pton(AF_INET, ip_address.c_str(), &server_addr.sin_addr);
//                 sockaddr_in cli_in_addr{};
//                 socklen_t cli_addr_len = sizeof(cli_in_addr);
//                 if (!is_client) {
//                     const auto server_fd = socket(AF_INET, SOCK_STREAM, 0);
//                     std::cout << "server_fd: " << server_fd << std::endl;
//                     fcntl(server_fd, F_SETFL, O_NONBLOCK);
//                     constexpr int opt = 1;
//                     setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
//                     sockaddr_in addr{};
//                     addr.sin_family = AF_INET;
//                     inet_pton(AF_INET, ip_address.c_str(), &addr.sin_addr);
//                     addr.sin_port = htons(base_port + threadId);
//
//                     if (bind(server_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
//                         perror("bind");
//                         return;
//                     }
//                     if (listen(server_fd, SOMAXCONN) < 0) {
//                         perror("listen");
//                         return;
//                     }
//
//                     // submit([cli_addr_len, &connection_index, &cli_in_addr](io_uring_sqe &sqe) {
//                     //     sqe.opcode = IORING_OP_ACCEPT;
//                     //     sqe.ioprio |= IORING_ACCEPT_MULTISHOT;
//                     //     sqe.fd = server_fd;
//                     //     sqe.off = &cli_addr_len;
//                     //     sqe.addr = reinterpret_cast<unsigned long>(&cli_in_addr);
//                     //     sqe.len = 0;
//                     //     sqe.user_data = pack_fd_index_opcode(server_fd, connection_index++, IORING_OP_ACCEPT);
//                     // });
//                 } else {
//                     for (int c = 0; c < clients_per_thread; c++) {
//                         const auto client_fd = socket(AF_INET, SOCK_STREAM, 0);
//                         fcntl(client_fd, F_SETFL, O_NONBLOCK);
//
//                         const auto result = connect(client_fd, reinterpret_cast<sockaddr *>(&server_addr), sizeof(server_addr));
//                         if (result == -1 && errno != EINPROGRESS && errno != EALREADY) {
//                             perror("connect");
//                             close(client_fd);
//                             continue;
//                         }
//
//                         int flag = 1;
//                         if (setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag)) < 0) {
//                             perror("setsockopt TCP_NODELAY failed");
//                         }
//
//                         submit([server_addr, &connection_index](io_uring_sqe& sqe) {
//                             sqe.opcode = IORING_OP_CONNECT;
//                             sqe.fd = client_fd;
//                             sqe.off = sizeof(server_addr);
//                             sqe.addr = reinterpret_cast<unsigned long>(&server_addr);
//                             sqe.len = 0;
//                             sqe.user_data = pack_fd_index_opcode(client_fd, connection_index++, IORING_OP_CONNECT);
//                         });
//                     }
//                 }
//
//                 while (active.load()) {
//                     const auto head = cq_head->load();
//                     const auto tail = cq_tail->load();
//                     const auto to_process = (tail - head);
//                     if (to_process > 0) {
//                         for (int i = 0; i < to_process; i++) {
//                             const auto index = (head + i) & *cq_ring_mask;
//                             const auto cq = cqes[index];
//                             int fd;
//                             uint32_t conn_index;
//                             uint8_t opcode;
//                             unpack_fd_index_opcode(cq.user_data, fd, conn_index, opcode);
//                             const auto user_data = cq.user_data;
//                             const auto response = cq.res;
//                             switch (opcode) {
//                                 case IORING_OP_CONNECT:
//                                     if (response < 0) {
//                                         std::cout << "Error connecting!" << std::endl;
//                                         submit([server_addr, &connection_index, user_data, fd](io_uring_sqe &sqe) {
//                                             sqe.opcode = IORING_OP_CONNECT;
//                                             sqe.fd = fd;
//                                             sqe.off = sizeof(server_addr);
//                                             sqe.addr = reinterpret_cast<unsigned long>(&server_addr);
//                                             sqe.len = 0;
//                                             sqe.user_data = user_data;
//                                         });
//                                     } else {
//                                         submit([fd, conn_index](io_uring_sqe &sqe) {
//                                             sqe.opcode = IORING_OP_READ_MULTISHOT;
//                                             sqe.fd = conn_index;
//                                             sqe.flags = (IOSQE_FIXED_FILE | IOSQE_BUFFER_SELECT);
//                                             sqe.buf_group = 0;
//                                             sqe.user_data = pack_fd_index_opcode(fd, conn_index, IORING_OP_READ_MULTISHOT);
//                                         });
//                                     }
//                                 case IORING_OP_ACCEPT:
//                                     if (response < 0) {
//                                         throw std::runtime_error("Error accepting!");
//                                     }
//
//                                     // register new socket
//
//                                     submit([fd, conn_index, response](io_uring_sqe &sqe) {
//                                         sqe.opcode = IORING_OP_READ_MULTISHOT;
//                                         sqe.fd = conn_index;
//                                         sqe.flags = (IOSQE_FIXED_FILE | IOSQE_BUFFER_SELECT);
//                                         sqe.buf_group = 0;
//                                         sqe.user_data = pack_fd_index_opcode(fd, response, IORING_OP_READ_MULTISHOT);
//                                     });
//
//                             }
//                             // if (op == OP_CONNECT) {
//                             //
//                             // }
//                         }
//
//                         cq_head->fetch_add(to_process);
//                     }
//
//                     if (to_submit > 0) {
//                         if ((sq_flags->load() & IORING_SQ_NEED_WAKEUP) != 0) {
//                             io_uring_enter(ring_fd, 0, 0, IORING_ENTER_SQ_WAKEUP);
//                         }
//                         to_submit = 0;
//                     }
//                 }
//
//                 close(ring_fd);
//             } catch (const std::runtime_error &e) {
//                 std::cerr << "error in thread " << threadId << ": " << e.what() << std::endl;
//
//             }
//         }).detach();
//     }
// }

int main(int argc, char *argv[]) {
    auto flags = parse_flags(argc, argv);

    if (!flags.contains("type") || !flags.contains("clients") || !flags.contains("threads") || !flags.contains("data") ||
        !flags.contains("events") || !flags.contains("host") || !flags.contains("client") || !flags.contains("ports") || !flags.contains("ports_count_up")) {
        std::cerr << "Usage:\n"
                << "  --type=0(io_uring)|1(epoll)\n"
                << "  --client=0(server)|1(client)\n"
                << "  --clients=N\n"
                << "  --threads=N\n"
                << "  --data=N\n"
                << "  --events=N\n"
                << "  --host=IP_ADDRESS\n"
                << "  --ports=PORTS\n"
                << "  --ports_count_up=0(no)|1(yes)\n";
        return 1;
    }

    const auto is_epoll = flags["type"] == "1";
    const auto is_client = flags["client"] == "1";
    const auto clients_per_thread = std::stoi(flags["clients"]);
    const auto threads = std::stoi(flags["threads"]);
    const auto data_size = std::stoi(flags["data"]);
    const auto max_events = std::stoi(flags["events"]);
    const auto ip_address = flags["host"];
    const auto port_by_thread = flags["ports"];
    const auto port_count_up = flags["ports_count_up"] == "1";
    const auto ports = split_ports(port_by_thread);

    if (ports.size() < 1) {
        perror("Must have at least 1 port!");
        return 1;
    }


    std::cout << "Ports size: " << ports.size() << std::endl;
    // if (ports.size() != threads) {
    //     perror("Ports size must be equal to threads!");
    //     return 1;
    // }

    std::signal(SIGINT, [](int) { active.store(false); });

    std::cout << "Total cores: " << std::thread::hardware_concurrency() << std::endl;

    active.store(true);
    if (is_epoll) {
        epoll_test(is_client, clients_per_thread, threads, data_size, max_events, ip_address, ports, port_count_up);
    } else {
        // iouring_test(is_client, clients_per_thread, threads, data_size, max_events, ip_address, ports);
    }

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
