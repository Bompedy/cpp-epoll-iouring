#pragma once
#include <array>
#include <atomic>
#include <memory>
#include <thread>
#include <utility>
#include <vector>
#include <sys/epoll.h>
#include "temp.h"


enum class Algorithm : uint8_t {
    MULTI_PAXOS,
    PAXOS,
    RABIA
};
enum class IOType : uint8_t {
    IO_URING,
    EPOLL
};

class Address {
    std::string host_;
    unsigned short port_;

    bool operator==(const Address& other) const {
        return host_ == other.host_ && port_ == other.port_;
    }
public:
    Address(std::string host, const unsigned short port) : host_(std::move(host)), port_(port) {}
    const std::string& host() const { return host_; }
    unsigned short port() const { return port_; }
};

struct InstanceConfig {
    Address host_config;
    std::vector<Address> peers;
};

struct Connection {
    std::unique_ptr<char[]> read_buffer;
    std::unique_ptr<char[]> write_buffer;
    size_t read_pos;
    size_t write_pos;
    bool open;
    ~Connection();
};

template<size_t log_size>
class Consensus {
    std::array<std::unique_ptr<char[]>, log_size> log;
    std::atomic_char acks[log_size];
    std::atomic<int> committed{};
    std::atomic<int> consumed{};
    std::atomic<bool> running{true};
    std::vector<std::thread> threads;

    void epoll_provider(
        int id,
        Algorithm algo,
        const std::vector<InstanceConfig> &instance_configs,
        unsigned int total_pipes,
        unsigned int total_connections,
        size_t buffer_size
    );

    void io_uring_provider(
        int id,
        Algorithm algo,
        const std::vector<InstanceConfig> &instance_configs,
        unsigned int total_pipes,
        unsigned int total_connections,
        size_t buffer_size
    );
public:
    Consensus(
        int id,
        IOType io_type,
        Algorithm algo,
        const std::vector<InstanceConfig> &instance_configs,
        unsigned int total_pipes,
        unsigned int total_connections,
        size_t buffer_size
    );

    void shutdown();
    ~Consensus();
};

#include "consensus.tpp"

// #pragma once
// #include <array>
// #include <atomic>
// #include <cstring>
// #include <iostream>
// #include <memory>
// #include <thread>
// #include <vector>
// #include <sys/epoll.h>
// #include <arpa/inet.h>
// #include "temp.h"
//
//
//
// enum class Algorithm : uint8_t {
//     MULTI_PAXOS,
//     PAXOS,
//     RABIA
// };
//
// enum class IOType : uint8_t {
//     IO_URING,
//     EPOLL
// };
//
// struct Address {
//     std::string host;
//     short port;
// };
//
// struct InstanceConfig {
//     Address host_config;
//     std::vector<Address> peers;
// };
//
// struct Connection {
//     char* read_buffer;
//     char* write_buffer;
//     size_t read_pos;
//     size_t write_pos;
//     bool open;
//
//     ~Connection() {
//         std::cout << "Deleting connection?" << std::endl;
//         delete[] read_buffer;
//         delete[] write_buffer;
//     }
// };
//
//
// template<size_t log_size>
// class Consensus {
//     std::array<std::unique_ptr<char[]>, log_size> log_;
//     std::atomic_char acks_[log_size];
//     std::atomic<int> committed_{};
//     std::atomic<int> consumed_{};
//     std::atomic<bool> running_{true};
//     std::vector<std::thread> threads_{};
//
//     void epoll_provider(
//         const Algorithm algo,
//         const std::vector<InstanceConfig> &instance_configs,
//         const int total_pipes,
//         const int total_connections
//     );
//
//     void io_uring_provider(
//         const Algorithm algo,
//         const std::vector<InstanceConfig> &instance_configs,
//         const int total_pipes,
//         const int total_connections
//     );
//
// public:
//     Consensus(
//         const IOType io_type,
//         const Algorithm algo,
//         const std::vector<InstanceConfig> &instance_configs,
//         const int total_pipes,
//         const int total_connections,
//         const size_t buffer_size
//     ) {
//         for (size_t i = 0; i < log_size; ++i) {
//             std::atomic_init(&acks_[i], 0);
//             log_[i] = std::make_unique<char[]>(buffer_size);
//         }
//
//         threads_.emplace_back([this] {
//             try {
//                 while (running_.load()) {
//                     const auto committed = committed_.load();
//                     const auto consumed = consumed_.load();
//
//                     if (consumed < committed) {
//                         std::cout << "Consuming log index: " << consumed << std::endl;
//                         acks_[consumed % log_size].store(0);
//                         consumed_.store(consumed + 1);
//                     } else {
//                         std::this_thread::yield();
//                     }
//                 }
//             } catch (std::exception &e) {
//                 std::cerr << "Exception thrown: " << e.what() << std::endl;
//                 shutdown();
//             }
//         });
//
//         if (io_type == IOType::EPOLL) {
//             epoll_provider(algo, instance_configs, total_pipes, total_connections);
//         } else if (io_type == IOType::IO_URING) {
//             io_uring_provider(algo, instance_configs, total_pipes, total_connections);
//         }
//     }
//
//     void shutdown() {
//         running_.store(false);
//         for (auto &t : threads_) {
//             if (t.joinable()) {
//                 t.join();
//             }
//         }
//     }
//
//     ~Consensus() { shutdown(); }
// };
//
// template<size_t log_size>
// void Consensus<log_size>::epoll_provider(
//     const Algorithm algo,
//     const std::vector<InstanceConfig> &instance_configs,
//     const int total_pipes,
//     const int total_connections
// ) {
//     const auto connections_per_instance = total_connections / instance_configs.size();
//     const auto pipes_per_instance = total_pipes / instance_configs.size();
//
//     for (auto instance_id = 0; instance_id < instance_configs.size(); ++instance_id) {
//         const auto config = instance_configs[instance_id];
//         const auto pipe_offset = instance_id * pipes_per_instance;
//
//         threads_.emplace_back([&]() {
//             try {
//             } catch (std::exception &e) {
//                 std::cerr << "Exception thrown: " << e.what() << std::endl;
//                 shutdown();
//             }
//         });
//     }
// }
//
// template<size_t log_size>
// void Consensus<log_size>::io_uring_provider(
//     const Algorithm algo,
//     const std::vector<InstanceConfig> &instance_configs,
//     const int total_pipes,
//     const int total_connections
// ) {
//     const auto connections_per_instance = total_connections / instance_configs.size();
//     const auto pipes_per_instance = total_pipes / instance_configs.size();
//
//     for (auto instance_id = 0; instance_id < instance_configs.size(); ++instance_id) {
//         const auto config = instance_configs[instance_id];
//         const auto pipe_offset = instance_id * pipes_per_instance;
//
//         threads_.emplace_back([&]() {
//             try {
//             } catch (std::exception &e) {
//                 std::cerr << "Exception thrown: " << e.what() << std::endl;
//                 shutdown();
//             }
//         });
//     }
// }
//
//
// void epoll_pipes(
//     const Algorithm algo,
//     const std::vector<InstanceConfig>& instance_configs,
//     const int log_size,
//     const int total_pipes,
//     const int total_connections,
//     const size_t buffer_size
// ) {
//
//     const auto connections_per_instance = total_connections / instance_configs.size();
//     const auto pipes_per_instance = total_pipes / instance_configs.size();
//
//     for (auto instance_id = 0; instance_id < instance_configs.size(); ++instance_id) {
//         const auto& config = instance_configs[instance_id];
//         const auto pipe_offset = instance_id * pipes_per_instance;
//         std::thread([instance_id, algo, config, connections_per_instance, pipes_per_instance, buffer_size]() {
//             try {
//                 const auto pipe_offset = pipes_per_instance * instance_id;
//                 auto connection_index = 0;
//                 const int core_id = instance_id % std::thread::hardware_concurrency();
//                 pin_thread_to_core(core_id);
//
//                 std::vector<Connection *> connections;
//
//                 const auto epoll_fd = epoll_create1(0);
//                 epoll_event epoll_events[1024];
//                 const auto server_fd = setup_server_socket(config.host_config.host, config.host_config.port);
//                 std::cout << "Listening on: " << config.host_config.host << ":" << config.host_config.port << " " <<
//                         server_fd << std::endl;
//
//                 epoll_event server_event{};
//                 server_event.events = EPOLLIN | EPOLLET;
//                 server_event.data.u64 = pack_fd_and_index(server_fd, 0);
//                 epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &server_event);
//
//                 std::this_thread::sleep_for(std::chrono::seconds(2));
//                 for (auto [host, port]: config.peers) {
//                     sockaddr_in server_addr{};
//                     server_addr.sin_family = AF_INET;
//                     server_addr.sin_port = htons(port);
//                     inet_pton(AF_INET, host.c_str(), &server_addr.sin_addr);
//
//                     for (int i = 0; i < connections_per_instance; i++) {
//                         const auto client_fd = socket(AF_INET, SOCK_STREAM, 0);
//
//                         if (!tune_socket(client_fd)) {
//                             close(client_fd);
//                             throw std::runtime_error("Failed to create tune socket");
//                         }
//
//                         if (const auto result = connect(client_fd, reinterpret_cast<sockaddr *>(&server_addr),
//                                                         sizeof(server_addr));
//                             result == -1 && errno != EINPROGRESS && errno != EALREADY) {
//                             close(client_fd);
//                             throw std::runtime_error("Failed to connect");
//                         }
//
//                         const auto read_buffer = new char[buffer_size];
//                         const auto write_buffer = new char[buffer_size];
//
//                         std::cout << "Connecting to user: " << host << ":" << port << " " << client_fd << std::endl;
//
//                         connections.push_back(new Connection{read_buffer, write_buffer, 0, 0, true});
//                         epoll_event client_event{};
//                         client_event.events = EPOLLIN | EPOLLOUT | EPOLLET;
//                         client_event.data.u64 = pack_fd_and_index(client_fd, connection_index++);
//                         epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &client_event);
//                     }
//                 }
//
//                 while (true) {
//                     const auto n = epoll_wait(epoll_fd, epoll_events, 1024, -1);
//                     for (int event_id = 0; event_id < n; event_id++) {
//                         const auto event = epoll_events[event_id];
//                         const auto fd_and_index = epoll_events[event_id].data.u64;
//                         int fd;
//                         unsigned int index;
//                         unpack_fd_and_index(fd_and_index, fd, index);
//
//                         if (fd == server_fd) {
//                             while (true) {
//                                 sockaddr_in client_addr{};
//                                 socklen_t addr_len = sizeof(client_addr);
//                                 const auto client_fd = accept(server_fd, reinterpret_cast<sockaddr *>(&client_addr),
//                                                               &addr_len);
//                                 if (client_fd < 0) {
//                                     if (errno == EAGAIN || errno == EWOULDBLOCK) {
//                                         break;
//                                     }
//                                     throw std::runtime_error("error accepting on server!");
//                                 }
//
//                                 if (!tune_socket(client_fd)) {
//                                     close(client_fd);
//                                     throw std::runtime_error("failed to tune client socket!");
//                                 }
//
//                                 const auto read_buffer = new char[65535];
//                                 const auto write_buffer = new char[65535];
//                                 connections.emplace_back(new Connection{read_buffer, write_buffer, 0, 0, true});
//                                 std::cout << "Accepted connection on " << config.host_config.port << std::endl;
//                                 epoll_event client_event{};
//                                 client_event.events = EPOLLIN | EPOLLET | EPOLLOUT;
//                                 client_event.data.u64 = pack_fd_and_index(client_fd, connection_index++);
//                                 epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &client_event);
//                             }
//                         } else {
//                             auto conn = connections[index];
//
//                             if (event.events & EPOLLERR || event.events & EPOLLHUP || event.events & EPOLLRDHUP) {
//                                 std::cout << "EPOLLERR? " << (event.events & EPOLLERR)
//                                         << " HUP? " << (event.events & EPOLLHUP)
//                                         << " RDHUP? " << (event.events & EPOLLRDHUP)
//                                         << std::endl;
//                                 close(fd);
//                                 continue;
//                             }
//
//                             if (event.events & EPOLLIN) {
//                                 while (true) {
//                                     const auto bytes_read = read(fd, conn->read_buffer + conn->read_pos,
//                                                                  buffer_size - conn->read_pos);
//                                     if (bytes_read == -1) {
//                                         if (errno == EAGAIN || errno == EWOULDBLOCK) {
//                                             break;
//                                         }
//                                         close(fd);
//                                         throw std::runtime_error("error reading on socket!");
//                                     }
//
//                                     if (bytes_read == 0) {
//                                         close(fd);
//                                         break;
//                                     }
//
//                                     conn->read_pos += bytes_read;
//
//                                     while (true) {
//                                         if (conn->read_pos < 4) {
//                                             break;
//                                         }
//
//                                         unsigned int size;
//                                         std::memcpy(&size, conn->read_buffer, 4);
//                                         std::cout << "Got size " << size << " ntohl(" << ntohl(size) << size << ")" <<
//                                                 std::endl;
//
//
//                                         if (conn->read_pos < size + 4) {
//                                             break;
//                                         }
//
//                                         if (conn->read_pos > size + 4) {
//                                             memmove(conn->read_buffer, conn->read_buffer + size + 4,
//                                                     conn->read_pos - (size + 4));
//                                         }
//                                         conn->read_pos -= size + 4;
//                                     }
//                                 }
//                             }
//
//                             if (event.events & EPOLLOUT) {
//                                 while (conn->write_pos < 9) {
//                                     const auto bytes_written = write(fd, conn->write_buffer + conn->write_pos,
//                                                                      9 - conn->write_pos);
//                                     if (bytes_written == -1) {
//                                         if (errno == EAGAIN || errno == EWOULDBLOCK) {
//                                             break;
//                                         }
//
//                                         close(fd);
//                                         throw std::runtime_error("error writing to socket!");
//                                     }
//                                     conn->write_pos += bytes_written;
//                                 }
//                                 conn->write_pos = 0;
//                                 const auto payload_size = 5;
//                                 std::memcpy(conn->write_buffer, &payload_size, 4);
//                             }
//                         }
//                     }
//                 }
//             } catch (std::exception &e) {
//                 std::cerr << "Exception: " << e.what() << std::endl;
//             }
//         }).detach();
//     }
// }
//
// // constexpr size_t MAX_FDS = 1024;
// //
// // void io_uring_pipes(
// //     const Algorithm algo,
// //     const std::vector<InstanceConfig> &instance_configs,
// //     const int total_pipes,
// //     const int total_connections,
// //     const size_t buffer_size
// // ) {
// //     for (int threadId = 0; threadId < threads; threadId++) {
// //         const auto port = ports_count_up ? (ports[0] + threadId) : ports[threadId % ports.size()];
// //         std::thread([threadId, clients_per_thread, max_events, data_size, port, ip_address, is_client]() {
// //             try {
// //                 constexpr auto params = new io_uring_params();
// //                 params->flags |= IORING_SETUP_SQPOLL | IORING_SETUP_SINGLE_ISSUER;
// //                 const auto ring_fd = io_uring_setup(1024, params);
// //                 const auto sq_ring_size = params->sq_off.array + params->sq_entries + sizeof(__u32);
// //                 const auto sq_ptr = mmap(nullptr, sq_ring_size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ring_fd, IORING_OFF_SQ_RING);
// //                 if (sq_ptr == MAP_FAILED) throw std::runtime_error("mmap failed on sq_ptr");
// //                 const auto sqes_size = params->sq_entries * sizeof(io_uring_sqe);
// //                 const auto sqes = static_cast<struct io_uring_sqe *>(mmap(nullptr, sqes_size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ring_fd,IORING_OFF_SQES));
// //                 if (sqes == MAP_FAILED) throw std::runtime_error("mmap failed on sqes");
// //                 const auto cq_ring_size = params->cq_off.cqes + params->cq_entries * sizeof(struct io_uring_cqe);
// //                 const auto cq_ptr = mmap(NULL, cq_ring_size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ring_fd, IORING_OFF_CQ_RING);
// //                 if (cq_ptr == MAP_FAILED) throw std::runtime_error("mmap failed on cqes");
// //
// //                 const auto cq_head = reinterpret_cast<std::atomic<uint32_t> *>(static_cast<char *>(cq_ptr) + params->cq_off.head);
// //                 const auto cq_tail = reinterpret_cast<std::atomic<uint32_t> *>(static_cast<char *>(cq_ptr) + params->cq_off.tail);
// //                 const auto cq_ring_mask = reinterpret_cast<uint32_t *>(static_cast<char *>(cq_ptr) + params->cq_off.ring_mask);
// //                 const auto *cqes = reinterpret_cast<struct io_uring_cqe *>(static_cast<char *>(cq_ptr) + params->cq_off.cqes);
// //
// //                 const auto sq_head = reinterpret_cast<std::atomic<uint32_t>*>(static_cast<char*>(sq_ptr) + params->sq_off.head);
// //                 const auto sq_tail = reinterpret_cast<std::atomic<uint32_t>*>(static_cast<char*>(sq_ptr) + params->sq_off.tail);
// //                 const auto sq_ring_mask = reinterpret_cast<uint32_t*>(static_cast<char*>(sq_ptr) + params->sq_off.ring_mask);
// //                 const auto sq_array = reinterpret_cast<uint32_t *>(static_cast<char *>(sq_ptr) + params->sq_off.array);
// //                 const auto sq_flags = reinterpret_cast<std::atomic<uint32_t> *>(
// //                     static_cast<char *>(sq_ptr) + params->sq_off.flags);
// //
// //                 auto connection_index = 0;
// //                 std::vector<Connection> connections;
// //                 connections.resize(clients_per_thread + 100);
// //                 for (auto &conn: connections) {
// //                     conn->read_buffer = new char[data_size];
// //                     conn->write_buffer = new char[data_size];
// //                     conn->should_write = true;
// //                     conn->read_pos = conn->write_pos = 0;
// //                     conn->fd = -1;
// //                 }
// //
// //                 const auto fd_slots = new int[MAX_FDS];
// //                 io_uring_register(ring_fd, IORING_REGISTER_FILES, fd_slots, MAX_FDS);
// //
// //
// //                 auto to_submit = 0;
// //
// //                 auto submit = [sq_head, sq_tail, sq_ring_mask, sqes, &to_submit](const std::function<void(io_uring_sqe&)> &callback) {
// //                     const auto head = sq_head->load();
// //                     const auto tail = sq_tail->load();
// //                     const auto used = tail - head;
// //                     const auto space = params->sq_entries - used;
// //                     if (space <= 0) throw std::runtime_error("out of space in submission queue!");
// //                     const auto index = tail & *sq_ring_mask;
// //                     io_uring_sqe* entry = &sqes[index];
// //                     memset(entry, 0, sizeof(*entry));
// //                     callback(*entry);
// //                     to_submit += 1;
// //                     sq_tail->store(tail + 1, std::memory_order_seq_cst);
// //                 };
// //
// //
// //                 sockaddr_in server_addr{};
// //                 server_addr.sin_family = AF_INET;
// //                 server_addr.sin_port = htons(port);
// //                 inet_pton(AF_INET, ip_address.c_str(), &server_addr.sin_addr);
// //
// //                 sockaddr_in cli_in_addr{};
// //                 socklen_t cli_addr_len = sizeof(cli_in_addr);
// //                 if (!is_client) {
// //                     const auto server_fd = socket(AF_INET, SOCK_STREAM, 0);
// //                     std::cout << "server_fd: " << server_fd << std::endl;
// //                     fcntl(server_fd, F_SETFL, O_NONBLOCK);
// //                     constexpr int opt = 1;
// //                     setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
// //                     sockaddr_in addr{};
// //                     addr.sin_family = AF_INET;
// //                     inet_pton(AF_INET, ip_address.c_str(), &addr.sin_addr);
// //                     addr.sin_port = htons(port);
// //
// //                     if (bind(server_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
// //                         perror("bind");
// //                         return;
// //                     }
// //                     if (listen(server_fd, SOMAXCONN) < 0) {
// //                         perror("listen");
// //                         return;
// //                     }
// //
// //                     submit([cli_addr_len, &connection_index, &cli_in_addr](io_uring_sqe &sqe) {
// //                         sqe.opcode = IORING_OP_ACCEPT;
// //                         sqe.ioprio |= IORING_ACCEPT_MULTISHOT;
// //                         sqe.fd = server_fd;
// //                         sqe.off = &cli_addr_len;
// //                         sqe.addr = reinterpret_cast<unsigned long>(&cli_in_addr);
// //                         sqe.len = 0;
// //                         sqe.user_data = pack_fd_index_opcode(server_fd, connection_index++, IORING_OP_ACCEPT);
// //                     });
// //                 } else {
// //                     for (int c = 0; c < clients_per_thread; c++) {
// //                         const auto client_fd = socket(AF_INET, SOCK_STREAM, 0);
// //                         fcntl(client_fd, F_SETFL, O_NONBLOCK);
// //
// //                         const auto result = connect(client_fd, reinterpret_cast<sockaddr *>(&server_addr), sizeof(server_addr));
// //                         if (result == -1 && errno != EINPROGRESS && errno != EALREADY) {
// //                             perror("connect");
// //                             close(client_fd);
// //                             continue;
// //                         }
// //
// //                         int flag = 1;
// //                         if (setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag)) < 0) {
// //                             perror("setsockopt TCP_NODELAY failed");
// //                         }
// //
// //                         submit([server_addr, &connection_index](io_uring_sqe& sqe) {
// //                             sqe.opcode = IORING_OP_CONNECT;
// //                             sqe.fd = client_fd;
// //                             sqe.off = sizeof(server_addr);
// //                             sqe.addr = reinterpret_cast<unsigned long>(&server_addr);
// //                             sqe.len = 0;
// //                             sqe.user_data = pack_fd_index_opcode(client_fd, connection_index++, IORING_OP_CONNECT);
// //                         });
// //                     }
// //                 }
// //
// //                 while (active.load()) {
// //                     const auto head = cq_head->load();
// //                     const auto tail = cq_tail->load();
// //                     const auto to_process = (tail - head);
// //                     if (to_process > 0) {
// //                         for (int i = 0; i < to_process; i++) {
// //                             const auto index = (head + i) & *cq_ring_mask;
// //                             const auto cq = cqes[index];
// //                             int fd;
// //                             uint32_t conn_index;
// //                             uint8_t opcode;
// //                             unpack_fd_index_opcode(cq.user_data, fd, conn_index, opcode);
// //                             const auto user_data = cq.user_data;
// //                             const auto response = cq.res;
// //                             switch (opcode) {
// //                                 case IORING_OP_CONNECT:
// //                                     if (response < 0) {
// //                                         std::cout << "Error connecting!" << std::endl;
// //                                         submit([server_addr, &connection_index, user_data, fd](io_uring_sqe &sqe) {
// //                                             sqe.opcode = IORING_OP_CONNECT;
// //                                             sqe.fd = fd;
// //                                             sqe.off = sizeof(server_addr);
// //                                             sqe.addr = reinterpret_cast<unsigned long>(&server_addr);
// //                                             sqe.len = 0;
// //                                             sqe.user_data = user_data;
// //                                         });
// //                                     } else {
// //                                         submit([fd, conn_index](io_uring_sqe &sqe) {
// //                                             sqe.opcode = IORING_OP_READ;
// //                                             sqe.fd = conn_index;
// //                                             sqe.flags = (IOSQE_FIXED_FILE | IOSQE_BUFFER_SELECT);
// //                                             sqe.buf_group = 0;
// //                                             sqe.user_data = pack_fd_index_opcode(fd, conn_index, IORING_OP_READ_MULTISHOT);
// //                                         });
// //                                     }
// //                                     break;
// //                                 case IORING_OP_ACCEPT:
// //                                     if (response < 0) {
// //                                         throw std::runtime_error("Error accepting!");
// //                                     }
// //
// //                                     // register new socket
// //
// //                                     submit([fd, conn_index, response](io_uring_sqe &sqe) {
// //                                         sqe.opcode = IORING_OP_READ_MULTISHOT;
// //                                         sqe.fd = conn_index;
// //                                         sqe.flags = (IOSQE_FIXED_FILE | IOSQE_BUFFER_SELECT);
// //                                         sqe.buf_group = 0;
// //                                         sqe.user_data = pack_fd_index_opcode(fd, response, IORING_OP_READ_MULTISHOT);
// //                                     });
// //                                     break;
// //                                 default: ;
// //                             }
// //                         }
// //
// //                         cq_head->fetch_add(to_process);
// //                     }
// //
// //                     if (to_submit > 0) {
// //                         if ((sq_flags->load() & IORING_SQ_NEED_WAKEUP) != 0) {
// //                             io_uring_enter(ring_fd, 0, 0, IORING_ENTER_SQ_WAKEUP);
// //                         }
// //                         to_submit = 0;
// //                     }
// //                 }
// //
// //                 close(ring_fd);
// //             } catch (const std::runtime_error &e) {
// //                 std::cerr << "error in thread " << threadId << ": " << e.what() << std::endl;
// //
// //             }
// //         }).detach();
// //     }
// // }