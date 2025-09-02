#pragma once
#include <cstring>
#include <functional>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/mman.h>
#include <linux/io_uring.h>

#include "consensus.h"

inline Connection::~Connection() = default;

template<size_t log_size>
Consensus<log_size>::Consensus(
    const int id,
    const IOType io_type,
    const Algorithm algo,
    const std::vector<InstanceConfig> &instance_configs,
    const unsigned int total_pipes,
    const unsigned int total_connections,
    const size_t buffer_size
) {
    for (size_t i = 0; i < log_size; ++i) {
        std::atomic_init(&acks[i], 0);
        log[i] = std::make_unique<char[]>(buffer_size);
    }

    threads.emplace_back([this] {
        try {
            while (running.load()) {
                const auto current_commit = committed.load();
                const auto current_consume = consumed.load();

                if (current_consume < current_commit) {
                    const auto next = current_consume + 1;
                    std::cout << "Consuming log index: " << next << std::endl;
                    acks[next % log_size].store(0);
                    consumed.store(next);
                } else {
                    std::this_thread::yield();
                }
            }
        } catch (const std::exception &e) {
            std::cerr << "Exception thrown: " << e.what() << std::endl;
            shutdown();
        }
    });

    if (io_type == IOType::EPOLL) {
        epoll_provider(id, algo, instance_configs, total_pipes, total_connections, buffer_size);
    } else if (io_type == IOType::IO_URING) {
        std::cout << "Starting with id: " << id << std::endl;
        io_uring_provider(id, algo, instance_configs, total_pipes, total_connections, buffer_size);
    }
}

template<size_t log_size>
void Consensus<log_size>::shutdown() {
    if (running.exchange(false)) {
        for (auto &t : threads) {
            if (t.joinable()) {
                t.join();
            }
        }
    }
}

template<size_t log_size>
Consensus<log_size>::~Consensus() {
    shutdown();
}

template<size_t log_size>
void Consensus<log_size>::epoll_provider(
    int id,
    const Algorithm algo,
    const std::vector<InstanceConfig> &instance_configs,
    const unsigned int total_pipes,
    const unsigned int total_connections,
    const size_t buffer_size
) {
    for (int thread_id = 0; thread_id < instance_configs.size(); ++thread_id) {
        threads.emplace_back([&]() {
            const int num_pipes = total_pipes / instance_configs.size();
            const int num_connections = total_connections / instance_configs.size();
            const auto& config = instance_configs[thread_id];
            try {
                pin_thread_to_core(thread_id % std::thread::hardware_concurrency());
                const auto epoll_fd = epoll_create1(0);
                epoll_event epoll_events[512];
                const auto server_fd = setup_server_socket(config.host_config.host(), config.host_config.port());
                std::cout << thread_id << ": listening on " << config.host_config.host() << ":" << config.host_config.port() << " " << server_fd << std::endl;
                epoll_event server_event{};
                server_event.events = EPOLLIN | EPOLLET;
                server_event.data.u64 = pack_fd_and_index(server_fd, 0);
                epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &server_event);

                std::this_thread::sleep_for(std::chrono::seconds(5));

                for (const auto& address: config.peers) {
                     sockaddr_in server_addr{};
                     server_addr.sin_family = AF_INET;
                     server_addr.sin_port = htons(address.port());
                     inet_pton(AF_INET, address.host().c_str(), &server_addr.sin_addr);

                     for (int i = 0; i < num_connections; i++) {
                         const auto client_fd = socket(AF_INET, SOCK_STREAM, 0);

                         if (!tune_socket(client_fd)) {
                             close(client_fd);
                             throw std::runtime_error("Failed to create tune socket");
                         }

                         if (const auto result = connect(client_fd, reinterpret_cast<sockaddr *>(&server_addr),
                                                         sizeof(server_addr));
                             result == -1 && errno != EINPROGRESS && errno != EALREADY) {
                             close(client_fd);
                             throw std::runtime_error("Failed to connect");
                         }

                         const auto read_buffer = new char[buffer_size];
                         const auto write_buffer = new char[buffer_size];

                         // std::cout << "Connecting to user: " << host << ":" << port << " " << client_fd << std::endl;
                         //
                         // const auto connection = new Connection{read_buffer, write_buffer, 0, 0, true};
                         //
                         // connections.push_back(new Connection{read_buffer, write_buffer, 0, 0, true});
                         // epoll_event client_event{};
                         // client_event.events = EPOLLIN | EPOLLOUT | EPOLLET;
                         // client_event.data.u64 = pack_fd_and_index(client_fd, connection);
                         // epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &client_event);
                     }
                 }
            } catch (std::exception &e) {
                std::cerr << "Exception thrown: " << e.what() << std::endl;
            }
        });
    }
}


template<size_t log_size>
void Consensus<log_size>::io_uring_provider(
    int id,
    const Algorithm algo,
    const std::vector<InstanceConfig> &instance_configs,
    const unsigned int total_pipes,
    const unsigned int total_connections,
    const size_t buffer_size
) {
    auto& running_ref = running;

    for (int thread_id = 0; thread_id < (int)instance_configs.size(); ++thread_id) {
        threads.emplace_back(
            [thread_id, &instance_configs, total_pipes, total_connections, buffer_size, &running_ref, id]() {

                int socket_index = 0;
                const int num_pipes = total_pipes / instance_configs.size();
                const int num_connections = total_connections / instance_configs.size();
                const auto& config = instance_configs[thread_id];
                std::unordered_map<unsigned int, std::shared_ptr<sockaddr_in>> client_targets;

                try {
                    pin_thread_to_core(thread_id % std::thread::hardware_concurrency());

                    const auto params = new io_uring_params();
                    params->flags |= IORING_SETUP_SQPOLL | IORING_SETUP_SQ_AFF;
                    params->sq_thread_cpu = thread_id % std::thread::hardware_concurrency();
                    const auto ring_fd = io_uring_setup(1024, params);
                    if (ring_fd < 0) throw std::runtime_error("io_uring_setup failed");

                    const auto sq_ring_size = params->sq_off.array + params->sq_entries * sizeof(__u32);
                    const auto sq_ptr = mmap(nullptr, sq_ring_size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ring_fd, IORING_OFF_SQ_RING);
                    if (sq_ptr == MAP_FAILED) throw std::runtime_error("mmap failed on sq_ptr");

                    const auto sqes_size = params->sq_entries * sizeof(io_uring_sqe);
                    const auto sqes = static_cast<struct io_uring_sqe*>(mmap(nullptr, sqes_size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ring_fd, IORING_OFF_SQES));
                    if (sqes == MAP_FAILED) throw std::runtime_error("mmap failed on sqes");

                    const auto cq_ring_size = params->cq_off.cqes + params->cq_entries * sizeof(struct io_uring_cqe);
                    const auto cq_ptr = mmap(nullptr, cq_ring_size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ring_fd, IORING_OFF_CQ_RING);
                    if (cq_ptr == MAP_FAILED) throw std::runtime_error("mmap failed on cqes");

                    const auto cq_head = reinterpret_cast<std::atomic<uint32_t> *>(static_cast<char *>(cq_ptr) + params->cq_off.head);
                    const auto cq_tail = reinterpret_cast<std::atomic<uint32_t> *>(static_cast<char *>(cq_ptr) + params->cq_off.tail);
                    const auto cq_ring_mask = reinterpret_cast<uint32_t *>(static_cast<char *>(cq_ptr) + params->cq_off.ring_mask);
                    const auto *cqes = reinterpret_cast<struct io_uring_cqe *>(static_cast<char *>(cq_ptr) + params->cq_off.cqes);

                    const auto sq_head = reinterpret_cast<std::atomic<uint32_t>*>(static_cast<char*>(sq_ptr) + params->sq_off.head);
                    const auto sq_tail = reinterpret_cast<std::atomic<uint32_t>*>(static_cast<char*>(sq_ptr) + params->sq_off.tail);
                    const auto sq_ring_mask = reinterpret_cast<uint32_t*>(static_cast<char*>(sq_ptr) + params->sq_off.ring_mask);
                    const auto sq_array = reinterpret_cast<uint32_t *>(static_cast<char *>(sq_ptr) + params->sq_off.array);
                    const auto sq_flags = reinterpret_cast<std::atomic<uint32_t> *>(static_cast<char *>(sq_ptr) + params->sq_off.flags);

                    const auto fd_slots = new int[1000];
                    io_uring_register(ring_fd, IORING_REGISTER_FILES, fd_slots, 1000);

                    constexpr auto buffer_count = 1000;
                    constexpr auto total_buffers = buffer_count * 2;

                    std::vector<std::unique_ptr<char[]>> buffers(total_buffers);
                    std::vector<iovec> iovecs(total_buffers);

                    for (int i = 0; i < total_buffers; i++) {
                        buffers[i] = std::make_unique<char[]>(buffer_size);
                        iovecs[i].iov_base = buffers[i].get();
                        iovecs[i].iov_len = buffer_size;
                    }

                    io_uring_register(ring_fd, IORING_REGISTER_BUFFERS, iovecs.data(), total_buffers);

                    auto register_socket = [&](const int socket_fd) -> unsigned int {
                        const auto next_index = socket_index++;

                        fd_slots[next_index] = socket_fd;

                        io_uring_files_update update{};
                        update.offset = next_index;
                        update.resv = 0;
                        update.fds = reinterpret_cast<__u64>(&fd_slots[next_index]);

                        if (const int result = io_uring_register(ring_fd, IORING_REGISTER_FILES_UPDATE, &update, 1);
                            result < 0) {
                            throw std::runtime_error(std::string("register socket io_uring_register failed: ") + strerror(errno));
                        }

                        return next_index;
                    };


                    int to_submit = 0;
                    auto submit = [&](const std::function<void(io_uring_sqe &)> &callback) {
                        const auto head = sq_head->load(std::memory_order_acquire);
                        const auto tail = sq_tail->load(std::memory_order_acquire);
                        const auto used = tail - head;
                        if (const auto space = params->sq_entries - used; space <= 0) {
                            throw std::runtime_error("out of space in submission queue!");
                        }
                        const auto index = tail & *sq_ring_mask;
                        io_uring_sqe *entry = &sqes[index];
                        sq_array[index] = index;
                        memset(entry, 0, sizeof(*entry));
                        callback(*entry);
                        to_submit++;

                        sq_tail->store(tail + 1, std::memory_order_release);
                    };


                    sockaddr_in server_addr{};
                    server_addr.sin_family = AF_INET;
                    server_addr.sin_port = htons(config.host_config.port());
                    inet_pton(AF_INET, config.host_config.host().c_str(), &server_addr.sin_addr);

                    const auto server_fd = socket(AF_INET, SOCK_STREAM, 0);
                    if (!tune_socket(server_fd)) {
                        throw std::runtime_error("server tune_socket failed");
                    }

                    if (bind(server_fd, reinterpret_cast<sockaddr *>(&server_addr), sizeof(server_addr)) < 0) {
                        throw std::runtime_error("bind failed");
                    }

                    if (listen(server_fd, SOMAXCONN) < 0) {
                        throw std::runtime_error("listen failed");
                    }

                    sockaddr_in cli_in_addr{};
                    socklen_t cli_addr_len = sizeof(cli_in_addr);

                    submit([server_fd, &cli_addr_len, &cli_in_addr](io_uring_sqe &sqe) {
                        sqe.opcode = IORING_OP_ACCEPT;
                        sqe.fd = server_fd;
                        sqe.addr = reinterpret_cast<__u64>(&cli_in_addr);
                        sqe.off = reinterpret_cast<__u64>(&cli_addr_len);
                        sqe.len = 0;
                        sqe.user_data = pack_fd_index_opcode(server_fd, 0, IORING_OP_ACCEPT);
                    });

                    for (const auto &peer_address: config.peers) {
                        std::cout << "Gonna connect from: " << config.host_config.host() << ":" << config.host_config.port() << " to " << peer_address.host() << ":"  << peer_address.port()<< std::endl;
                        sockaddr_in target_addr{};
                        target_addr.sin_family = AF_INET;
                        target_addr.sin_port = htons(peer_address.port());
                        inet_pton(AF_INET, peer_address.host().c_str(), &target_addr.sin_addr);
                        auto target_ptr = std::make_shared<sockaddr_in>(target_addr);

                        for (int i = 0; i < num_connections; i++) {
                            const auto client_fd = socket(AF_INET, SOCK_STREAM, 0);
                            if (!tune_socket(client_fd)) {
                                throw std::runtime_error("client tune_socket failed");
                            }

                            const auto client_index = register_socket(client_fd);
                            client_targets[client_index] = target_ptr;
                            submit([client_fd, client_index, target_addr_copy=target_addr, target_ptr](io_uring_sqe &sqe) {
                               sqe.opcode = IORING_OP_CONNECT;
                               sqe.fd = client_fd;
                               sqe.off = sizeof(sockaddr_in);
                               sqe.addr = reinterpret_cast<__u64>(target_ptr.get());
                               sqe.len = 0;
                               sqe.user_data = pack_fd_index_opcode(client_fd, client_index, IORING_OP_CONNECT);
                           });
                        }
                    }

                    while (running_ref.load(std::memory_order_acquire)) {
                        const auto head = cq_head->load(std::memory_order_acquire);
                        const auto tail = cq_tail->load(std::memory_order_acquire);
                        if (const auto to_process = tail - head; to_process > 0) {
                            for (int i = 0; i < to_process; i++) {
                                const auto index = (head + i) & *cq_ring_mask;
                                const auto cq = cqes[index];
                                int fd;
                                uint32_t conn_index;
                                uint8_t opcode;
                                unpack_fd_index_opcode(cq.user_data, fd, conn_index, opcode);
                                const auto response = cq.res;

                                switch (opcode) {
                                    case IORING_OP_CONNECT: {
                                        if (response < 0) {
                                            const auto target_ptr = client_targets[conn_index];
                                            submit([fd, target_ptr, user_data = cq.user_data](io_uring_sqe &sqe) {
                                                sqe.opcode = IORING_OP_CONNECT;
                                                sqe.fd = fd;
                                                sqe.off = sizeof(sockaddr_in);
                                                sqe.addr = reinterpret_cast<__u64>(target_ptr.get());
                                                sqe.len = 0;
                                                sqe.user_data = user_data;
                                            });
                                        } else {
                                            client_targets.erase(conn_index);
                                            submit([buf_ptr = buffers[conn_index].get(), buffer_size, fd, conn_index](io_uring_sqe &sqe) {
                                                sqe.opcode = IORING_OP_READ_FIXED;
                                                sqe.fd = static_cast<__s32>(conn_index);
                                                sqe.buf_index = conn_index;
                                                sqe.addr = reinterpret_cast<__u64>(buf_ptr);
                                                sqe.off = 0;
                                                sqe.len = buffer_size;
                                                sqe.flags = IOSQE_FIXED_FILE;
                                                sqe.user_data = pack_fd_index_opcode(fd, conn_index, IORING_OP_READ_FIXED);
                                            });
                                        }
                                        break;
                                    }

                                    case IORING_OP_ACCEPT: {
                                        if (response < 0) {
                                            throw std::runtime_error("Error accepting!");
                                        }

                                        std::cout << "Accepted a connection on " << config.host_config.host() << ":" << config.host_config.port() << " with socket " << response << std::endl;
                                        const auto client_socket = response;

                                        if (!tune_socket(client_socket)) {
                                            throw std::runtime_error("accepted client tune_socket failed");
                                        }

                                        const auto client_index = register_socket(client_socket);
                                        submit([server_fd, &cli_addr_len, &cli_in_addr](io_uring_sqe &sqe) {
                                            std::memset(&cli_in_addr, 0, sizeof(cli_in_addr));
                                            cli_addr_len = sizeof(cli_in_addr);
                                            sqe.opcode = IORING_OP_ACCEPT;
                                            sqe.fd = server_fd;
                                            sqe.addr = reinterpret_cast<__u64>(&cli_in_addr);
                                            sqe.off = reinterpret_cast<__u64>(&cli_addr_len);
                                            sqe.len = 0;
                                            sqe.user_data = pack_fd_index_opcode(server_fd, 0, IORING_OP_ACCEPT);
                                        });

                                        submit([buf_ptr = buffers[client_index].get(), buffer_size, client_socket, client_index](io_uring_sqe &sqe) {
                                            sqe.opcode = IORING_OP_READ_FIXED;
                                            sqe.fd = client_index;
                                            sqe.buf_index = client_index;
                                            sqe.addr = reinterpret_cast<__u64>(buf_ptr);
                                            sqe.off = 0;
                                            sqe.len = buffer_size;
                                            sqe.flags = IOSQE_FIXED_FILE;
                                            sqe.user_data = pack_fd_index_opcode(client_socket, client_index, IORING_OP_READ_FIXED);
                                        });
                                        break;
                                    }

                                    case IORING_OP_READ_FIXED: {
                                        if (response < 0) {
                                            throw std::runtime_error("Error reading from socket!");
                                        } else if (response == 0) {
                                            throw std::runtime_error("EOF reached");
                                        } else {
                                            // read from buffer here, parse packets
                                        }

                                        submit([buf_ptr = buffers[conn_index].get(), buffer_size, conn_index, user_data = cq.user_data](io_uring_sqe &sqe) {
                                                sqe.opcode = IORING_OP_READ_FIXED;
                                                sqe.fd = conn_index;
                                                sqe.buf_index = conn_index;
                                                sqe.addr = reinterpret_cast<__u64>(buf_ptr);
                                                sqe.off = 0; // need to set proper offset
                                                sqe.len = buffer_size;
                                                sqe.flags = IOSQE_FIXED_FILE;
                                                sqe.user_data = user_data;
                                            });
                                        break;
                                    }

                                    case IORING_OP_WRITE_FIXED: {
                                        if (response < 0) {
                                            throw std::runtime_error("Error writing to socket!");
                                        } else {
                                            // handle partial write, maybe write out more
                                        }
                                        break;
                                    }

                                    default: {
                                        std::cout << "Unhandled opcode " << opcode << std::endl;
                                        break;
                                    }
                                }
                            }
                            cq_head->fetch_add(to_process, std::memory_order_release);
                        }

                        if (to_submit > 0) {
                            if ((sq_flags->load() & IORING_SQ_NEED_WAKEUP) != 0) {
                                io_uring_enter(ring_fd, 0, 0, IORING_ENTER_SQ_WAKEUP);
                            }
                            to_submit = 0;
                        }
                    }

                    std::cout << "Breaking out or something : " << id << std::endl;

                    close(ring_fd);

                    // Clean up
                    delete[] fd_slots;
                    delete params;
                } catch (const std::runtime_error &e) {
                    std::cerr << "error in thread " << thread_id << ": " << e.what() << std::endl;
                }
            }
        ).detach();
    }
}

