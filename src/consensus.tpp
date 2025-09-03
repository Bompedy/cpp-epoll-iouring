#pragma once
#include <cstring>
#include <deque>
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
            const auto&[host_config, peers] = instance_configs[thread_id];
            try {
                pin_thread_to_core(thread_id % std::thread::hardware_concurrency());
                const auto epoll_fd = epoll_create1(0);
                epoll_event epoll_events[512];
                const auto server_fd = setup_server_socket(host_config.host(), host_config.port());
                std::cout << thread_id << ": listening on " << host_config.host() << ":" << host_config.port() << " " << server_fd << std::endl;
                epoll_event server_event{};
                server_event.events = EPOLLIN | EPOLLET;
                server_event.data.u64 = pack_fd_and_index(server_fd, 0);
                epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &server_event);

                std::this_thread::sleep_for(std::chrono::seconds(5));

                for (const auto& address: peers) {
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

                         if (const auto result = connect(client_fd, reinterpret_cast<sockaddr *>(&server_addr), sizeof(server_addr));
                             result == -1 && errno != EINPROGRESS && errno != EALREADY) {
                             close(client_fd);
                             throw std::runtime_error("Failed to connect");
                         }

                         // const auto read_buffer = new char[buffer_size];
                         // const auto write_buffer = new char[buffer_size];

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

class Buffer {
    Buffer(const size_t capacity) : capacity(capacity), data(std::make_unique<char[]>(capacity)) {

    }

    unsigned int read_head = 0;
    unsigned int write_head_a = 0;
    unsigned int write_head_b = 0;
    const size_t capacity;
    std::unique_ptr<char[]> data;

    template<typename T>
    void put_value(T value) {
        constexpr size_t size = sizeof(T);
        if (capacity - write_head_a < size) {
            if (read_head - write_head_b < size) {
                // write from write_head_b
            } else {
                throw std::runtime_error("Buffer overflow");
            }
        } else {
            // write form write_head_a
        }
    }

public:
    char* get_data() const {
        return data.get();
    }

    char get_byte() {}
    short get_short() {}
    int get_int() {}
    float get_float() {}
    long get_long() {}
    double get_double() {}
    char* get_bytes(const size_t size) {}

    void put_byte(const char value) { return put_value<char>(value); }
    void put_short(const short value) { return put_value<short>(value); }
    void put_int(const int value) { return put_value<int>(value); }
    void put_float(const float value) { return put_value<float>(value); }
    void put_long(const long value) { return put_value<long>(value); }
    void put_double(const double value) { return put_value<double>(value); }

    void put_bytes(const char* value, const size_t size) {

    }

    size_t get_capacity() const { return capacity; }
    size_t get_read_index() const { return read_head; }
};

class BufferTracker {
    bool write_in_progress = false;
    Buffer read_buffer;
    int write_buffer_index, read_buffer_index;
    Buffer write_buffer;
public:
    BufferTracker(const size_t buffer_capacity, const unsigned int read_buffer_index, const unsigned int write_buffer_index)
        : read_buffer(buffer_capacity),
          write_buffer_index(write_buffer_index),
          read_buffer_index(read_buffer_index),
          write_buffer(buffer_capacity) {
    }

    bool is_write_in_progress() const {
        return write_in_progress;
    }

    void set_write_in_progress(const bool value) {
        write_in_progress = value;
    }

    const Buffer& get_read_buffer() const {
        return read_buffer;
    }

    const Buffer& get_write_buffer() const {
        return write_buffer;
    }

    unsigned int get_write_buffer_index() const {
        return write_buffer_index;
    }

    unsigned int get_read_buffer_index() const {
        return read_buffer_index;
    }
};

struct IoUringContext {
    int ring_fd = -1;
    mutable int to_submit = 0;
    std::unordered_map<unsigned int, std::shared_ptr<sockaddr_in>> client_targets;

    std::unique_ptr<io_uring_params> params;
    void* sq_ptr = nullptr;
    void* cq_ptr = nullptr;
    io_uring_sqe* sqes = nullptr;
    io_uring_cqe* cqes = nullptr;

    std::atomic<uint32_t>* sq_head = nullptr;
    std::atomic<uint32_t>* sq_tail = nullptr;
    uint32_t* sq_ring_mask = nullptr;
    uint32_t* sq_array = nullptr;
    std::atomic<uint32_t>* sq_flags = nullptr;

    std::atomic<uint32_t>* cq_head = nullptr;
    std::atomic<uint32_t>* cq_tail = nullptr;
    uint32_t* cq_ring_mask = nullptr;

    int socket_index = 0;
    int* fd_slots = nullptr;

    static constexpr size_t buffer_count = 1000;

    std::vector<std::unique_ptr<BufferTracker>> trackers;

    void initialize(const size_t buffer_size, const int sq_entries = 1024) {
        params = std::make_unique<io_uring_params>();
        std::memset(params.get(), 0, sizeof(io_uring_params));
        params->flags |= IORING_SETUP_SQPOLL;
        params->sq_thread_idle = 1000000;

        ring_fd = io_uring_setup(sq_entries, params.get());
        if (ring_fd < 0) {
            throw std::runtime_error("io_uring_setup failed");
        }

        {
            const auto sq_ring_size = params->sq_off.array + params->sq_entries * sizeof(__u32);
            sq_ptr = mmap(nullptr, sq_ring_size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ring_fd, IORING_OFF_SQ_RING);
            if (sq_ptr == MAP_FAILED) throw std::runtime_error("mmap failed on sq_ptr");

            const auto sqes_size = params->sq_entries * sizeof(io_uring_sqe);
            sqes = static_cast<io_uring_sqe*>(mmap(nullptr, sqes_size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ring_fd, IORING_OFF_SQES));
            if (sqes == MAP_FAILED) throw std::runtime_error("mmap failed on sqes");
        }

        {
            const auto cq_ring_size = params->cq_off.cqes + params->cq_entries * sizeof(io_uring_cqe);
            cq_ptr = mmap(nullptr, cq_ring_size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ring_fd, IORING_OFF_CQ_RING);
            if (cq_ptr == MAP_FAILED) throw std::runtime_error("mmap failed on cq_ptr");
        }

        cq_head = reinterpret_cast<std::atomic<uint32_t>*>(static_cast<char *>(cq_ptr) + params->cq_off.head);
        cq_tail = reinterpret_cast<std::atomic<uint32_t>*>(static_cast<char *>(cq_ptr) + params->cq_off.tail);
        cq_ring_mask = reinterpret_cast<uint32_t*>(static_cast<char *>(cq_ptr) + params->cq_off.ring_mask);
        cqes = reinterpret_cast<io_uring_cqe*>(static_cast<char *>(cq_ptr) + params->cq_off.cqes);

        sq_head = reinterpret_cast<std::atomic<uint32_t>*>(static_cast<char *>(sq_ptr) + params->sq_off.head);
        sq_tail = reinterpret_cast<std::atomic<uint32_t>*>(static_cast<char *>(sq_ptr) + params->sq_off.tail);
        sq_ring_mask = reinterpret_cast<uint32_t*>(static_cast<char *>(sq_ptr) + params->sq_off.ring_mask);
        sq_array = reinterpret_cast<uint32_t*>(static_cast<char *>(sq_ptr) + params->sq_off.array);
        sq_flags = reinterpret_cast<std::atomic<uint32_t>*>(static_cast<char *>(sq_ptr) + params->sq_off.flags);

        fd_slots = new int[buffer_count];
        io_uring_register(ring_fd, IORING_REGISTER_FILES, fd_slots, buffer_count);

        io_vecs.resize(buffer_count * 2);
        for (int i = 0; i < buffer_count; ++i) {
            trackers[i] = std::make_unique<BufferTracker>(buffer_size, i, i+buffer_count);
            auto& tracker = *trackers[i];
            io_vecs[i].iov_base = tracker.get_read_buffer().get_data();
            io_vecs[i].iov_len = buffer_size;
            io_vecs[i+buffer_count].iov_base = tracker.get_write_buffer().get_data();
            io_vecs[i+buffer_count].iov_len = buffer_size;
        }

        io_uring_register(ring_fd, IORING_REGISTER_BUFFERS, io_vecs.data(), buffer_count*2);
    }

    ~IoUringContext() {
        if (ring_fd != -1) close(ring_fd);
        delete[] fd_slots;
        delete params;

        if (sq_ptr) munmap(sq_ptr, params->sq_off.array + params->sq_entries * sizeof(__u32));
        if (sqes) munmap(sqes, params->sq_entries * sizeof(io_uring_sqe));
        if (cq_ptr) munmap(cq_ptr, params->cq_off.cqes + params->cq_entries * sizeof(io_uring_cqe));
    }

    std::pair<io_uring_sqe*, uint32_t> get_sqe() const {
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
        to_submit += 1;
        return {entry, tail};
    }

    int register_socket(const unsigned int fd) {
        const auto next_index = socket_index++;
        fd_slots[next_index] = fd;

        io_uring_files_update update{};
        update.offset = next_index;
        update.resv = 0;
        update.fds = reinterpret_cast<__u64>(&fd_slots[next_index]);

        if (const int result = io_uring_register(ring_fd, IORING_REGISTER_FILES_UPDATE, &update, 1); result < 0) {
            throw std::runtime_error(std::string("register socket io_uring_register failed: ") + strerror(errno));
        }
        return next_index;
    }

    void submit_accept(
        const unsigned int server_fd,
        sockaddr_in &cli_in_addr,
        socklen_t &cli_addr_len
    ) const {
        const auto [sqe, tail] = get_sqe();
        sqe->opcode = IORING_OP_ACCEPT;
        sqe->fd = server_fd;
        sqe->addr = reinterpret_cast<__u64>(&cli_in_addr);
        sqe->off = reinterpret_cast<__u64>(&cli_addr_len);
        sqe->user_data = pack_fd_index_opcode(server_fd, 0, IORING_OP_ACCEPT);
        sq_tail->store(tail + 1, std::memory_order_release);
    }

    void submit_connect(
        const unsigned int client_fd,
        const unsigned int client_index,
        sockaddr_in *target_ptr
    ) const {
        const auto [sqe, tail] = get_sqe();
        sqe->opcode = IORING_OP_CONNECT;
        sqe->fd = client_fd;
        sqe->off = sizeof(sockaddr_in);
        sqe->addr = reinterpret_cast<__u64>(target_ptr);
        sqe->user_data = pack_fd_index_opcode(client_fd, client_index, IORING_OP_CONNECT);
        sq_tail->store(tail + 1, std::memory_order_release);
    }

    void submit_write(
        const unsigned int fd,
        const unsigned int conn_index,
        BufferTracker &tracker
    ) const {
        if (!tracker.is_write_in_progress()) {
            tracker.set_write_in_progress(true);
            const auto [sqe, tail] = get_sqe();
            sqe->opcode = IORING_OP_WRITE_FIXED;
            sqe->fd = static_cast<__s32>(conn_index);
            sqe->buf_index = tracker.get_write_buffer_index();
            // sqe->addr = reinterpret_cast<__u64>(write_buffer);
            sqe->off = 0;
            // sqe->len = size;
            sqe->flags = IOSQE_FIXED_FILE;
            sqe->user_data = pack_fd_index_opcode(fd, conn_index, IORING_OP_WRITE_FIXED);
            sq_tail->store(tail + 1, std::memory_order_release);
        }
    }

    void submit_read(
        const unsigned int fd,
        const unsigned int conn_index,
        const BufferTracker &tracker
    ) const {
        const auto [sqe, tail] = get_sqe();
        sqe->opcode = IORING_OP_READ_FIXED;
        sqe->fd = static_cast<__s32>(conn_index);
        sqe->buf_index = tracker.get_read_buffer_index();
        sqe->addr = reinterpret_cast<__u64>(tracker.get_read_buffer().get_data() + tracker.get_read_buffer().
                                            get_read_index());
        sqe->len = tracker.get_read_buffer().get_capacity() - tracker.get_read_buffer().get_read_index();
        sqe->off = 0;
        sqe->flags = IOSQE_FIXED_FILE;
        sqe->user_data = pack_fd_index_opcode(fd, conn_index, IORING_OP_READ_FIXED);
        sq_tail->store(tail + 1, std::memory_order_release);
    }

    void on_write(
        const unsigned int fd,
        const unsigned int conn_index,
        const int response
    ) const {
        auto &tracker = *trackers[conn_index];
        tracker.set_write_in_progress(false);
        // check if everything is written out, if not submit write

        //
        // auto write = [&buffer_trackers, submit, &buffers](const int fd, const int conn_index, const unsigned int size, char* data) {
        //     auto &tracker = *buffer_trackers[conn_index];
        //     char *write_buffer = buffers[conn_index + buffer_count].get();
        //     tracker.write_queue.push_back({data, 0, size});
        //     if (!tracker.write_in_progress) {
        //         tracker.write_in_progress = true;
        //         memmove(write_buffer, data, size);
        //         submit([conn_index, fd, &tracker, write_buffer, size](io_uring_sqe &sqe) {
        //             sqe.opcode = IORING_OP_WRITE_FIXED;
        //             sqe.fd = static_cast<__s32>(conn_index);
        //             sqe.buf_index = conn_index + buffer_count;
        //             sqe.addr = reinterpret_cast<__u64>(write_buffer);
        //             sqe.off = 0;
        //             sqe.len = size;
        //             sqe.flags = IOSQE_FIXED_FILE;
        //             sqe.user_data = pack_fd_index_opcode(fd, conn_index, IORING_OP_WRITE_FIXED);
        //         });
        //     }
        // };
    }

    void on_read(
        const unsigned int fd,
        const unsigned int conn_index,
        const int response
    ) {
        if (response < 0) {
            throw std::runtime_error("Error reading from socket!");
        }
        if (response == 0) {
            throw std::runtime_error("EOF reached");
        }

        auto &tracker = *trackers[conn_index];
        //
        // auto& tracker = *buffer_trackers[conn_index];
        // tracker.read_offset += response;
        // char* start_read_buffer = buffers[conn_index].get();
        // char* read_buffer = start_read_buffer;
        // auto completed_size = 0u;
        // while (true) {
        //     if (tracker.read_offset < 4) {
        //         break;
        //     }
        //     const auto size = *reinterpret_cast<uint32_t*>(read_buffer);
        //     if (tracker.read_offset < 4 + size) {
        //         break;
        //     }
        //     if (const auto op = read_buffer[4]; op == 0) {
        //
        //
        //     }
        //     tracker.read_offset -= 4 + size;
        //     read_buffer += 4 + size;
        //     completed_size += 4 + size;
        // }
        // if (tracker.read_offset > 0) {
        //     memmove(start_read_buffer, start_read_buffer + completed_size, tracker.read_offset);
        // }
        //
        // ++ops;
        // submit([buf_ptr = buffers[conn_index].get(), buffer_size, conn_index, user_data = cq.user_data, &tracker](io_uring_sqe &sqe) {
        //         sqe.opcode = IORING_OP_READ_FIXED;
        //         sqe.fd = static_cast<__s32>(conn_index);
        //         sqe.buf_index = conn_index;
        //         sqe.addr = reinterpret_cast<__u64>(buf_ptr + tracker.read_offset);
        //         sqe.off = 0;
        //         sqe.len = buffer_size - tracker.read_offset;
        //         sqe.flags = IOSQE_FIXED_FILE;
        //         sqe.user_data = user_data;
        // });

        submit_read(fd, conn_index, tracker);
    }

    void on_connect(
        const unsigned int fd,
        const unsigned int conn_index,
        const int response
    ) {
        if (response < 0) {
            const auto target_ptr = client_targets[conn_index];
            submit_connect(fd, conn_index, target_ptr.get());
        } else {
            client_targets.erase(conn_index);
            submit_read(fd, conn_index, *trackers[conn_index]);
        }
    }

    void on_accept(
        const unsigned int fd,
        const int response
    ) {
        if (response < 0) {
            throw std::runtime_error("Error accepting!");
        }

        const auto client_socket = response;
        if (!tune_socket(client_socket)) {
            throw std::runtime_error("accepted client tune_socket failed");
        }

        const auto client_index = register_socket(client_socket);

        sockaddr_in cli_in_addr{};
        socklen_t cli_addr_len = sizeof(cli_in_addr);
        submit_accept(fd, cli_in_addr, cli_addr_len);
        const auto &tracker = *trackers[client_index];
        submit_read(fd, client_index, tracker);
    }

private:
    std::vector<iovec> io_vecs;
};


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

    for (int thread_id = 0; thread_id < static_cast<int>(instance_configs.size()); ++thread_id) {
        threads.emplace_back(
            [thread_id, &instance_configs, total_pipes, total_connections, buffer_size, &running_ref, id]() {
                const int num_pipes = total_pipes / instance_configs.size();
                const int num_connections = total_connections / instance_configs.size();
                const auto&[host_config, peers] = instance_configs[thread_id];

                try {
                    const auto context = std::make_shared<IoUringContext>();
                    context->initialize(buffer_size);

                    sockaddr_in server_addr{};
                    server_addr.sin_family = AF_INET;
                    server_addr.sin_port = htons(host_config.port());
                    inet_pton(AF_INET, host_config.host().c_str(), &server_addr.sin_addr);

                    const auto server_fd = setup_server_socket(host_config.host(), host_config.port());

                    sockaddr_in cli_in_addr{};
                    socklen_t cli_addr_len = sizeof(cli_in_addr);
                    context->submit_accept(server_fd, cli_in_addr, cli_addr_len);


                    for (const auto &peer_address: peers) {
                        std::cout << "Gonna connect from: " << host_config.host() << ":" << host_config.port() << " to " << peer_address.host() << ":"  << peer_address.port()<< std::endl;
                        sockaddr_in target_addr{};
                        target_addr.sin_family = AF_INET;
                        target_addr.sin_port = htons(peer_address.port());
                        inet_pton(AF_INET, peer_address.host().c_str(), &target_addr.sin_addr);
                        const auto target_ptr = std::make_shared<sockaddr_in>(target_addr);

                        for (int i = 0; i < num_connections; i++) {
                            const auto client_fd = socket(AF_INET, SOCK_STREAM, 0);
                            if (!tune_socket(client_fd)) {
                                throw std::runtime_error("client tune_socket failed");
                            }

                            const auto client_index = context->register_socket(client_fd);
                            context->client_targets[client_index] = target_ptr;
                            context->submit_connect(client_fd, client_index, &target_addr);
                        }
                    }

                    while (running_ref.load(std::memory_order_acquire)) {
                        const auto head = context->cq_head->load(std::memory_order_acquire);
                        const auto tail = context->cq_tail->load(std::memory_order_acquire);
                        if (const auto to_process = tail - head; to_process > 0) {
                            for (int i = 0; i < to_process; i++) {
                                const auto index = (head + i) & *context->cq_ring_mask;
                                const auto cq = context->cqes[index];
                                int fd;
                                unsigned int conn_index;
                                unsigned char opcode;
                                unpack_fd_index_opcode(cq.user_data, fd, conn_index, opcode);
                                const auto response = cq.res;

                                switch (opcode) {
                                    case IORING_OP_CONNECT: {
                                        context->on_connect(fd, conn_index, response);
                                        break;
                                    }

                                    case IORING_OP_ACCEPT: {
                                         context->on_accept(fd, response);
                                        break;
                                    }

                                    case IORING_OP_READ_FIXED: {
                                        context->on_read(fd, conn_index, response);
                                        break;
                                    }

                                    case IORING_OP_WRITE_FIXED: {
                                         context->on_write(fd, conn_index, response);
                                        break;
                                    }

                                    default: {
                                        throw std::runtime_error("invalid opcode: " + std::to_string(opcode));
                                    }
                                }
                            }
                            context->cq_head->fetch_add(to_process, std::memory_order_release);
                        }

                        if (context->to_submit > 0) {
                            if ((context->sq_flags->load() & IORING_SQ_NEED_WAKEUP) != 0) {
                                io_uring_enter(context->ring_fd, 0, 0, IORING_ENTER_SQ_WAKEUP);
                            }
                            context->to_submit = 0;
                        }
                    }

                } catch (const std::runtime_error &e) {
                    std::cerr << "error in thread " << thread_id << ": " << e.what() << std::endl;
                }
            }
        ).detach();
    }
}

