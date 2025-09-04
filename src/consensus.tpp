#pragma once
#include <cstring>
#include <deque>
#include <functional>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/mman.h>
#include <linux/io_uring.h>

#include "consensus.h"


template<typename T>
concept AllowedType = std::is_same_v<T, char> ||
                      std::is_same_v<T, unsigned char> ||
                      std::is_same_v<T, short> ||
                      std::is_same_v<T, unsigned short> ||
                      std::is_same_v<T, int> ||
                      std::is_same_v<T, unsigned int> ||
                      std::is_same_v<T, long long> ||
                      std::is_same_v<T, unsigned long long> ||
                      std::is_same_v<T, float> ||
                      std::is_same_v<T, double>;

struct RingBuffer {
    explicit RingBuffer(const size_t capacity) : buffer(std::make_unique<char[]>(capacity)), capacity(capacity) {

    }

    std::pair<__u64, size_t> next_write() const {
        const auto hmod = h % capacity;
        const auto tmod = t % capacity;
        return {reinterpret_cast<__u64>(buffer.get() + tmod), (hmod < tmod) ? (capacity - tmod) : (hmod - tmod)};
    }

    std::pair<__u64, size_t> next_read() const {
        const auto hmod = h % capacity;
        const auto tmod = t % capacity;
        return {reinterpret_cast<__u64>(buffer.get() + hmod), (tmod <= hmod) ? (capacity - hmod) : (tmod - hmod)};
    }

    template<AllowedType T>
    void put(const T& value) {
        const auto size = sizeof(T);
        if ((h + size) - t >= capacity) {
            throw std::runtime_error("BUFFER OVERFLOW");
        }
        if (const auto hmod = h % capacity; hmod + size > capacity) {
            const auto value_bytes = reinterpret_cast<const char*>(&value);
            std::memcpy(buffer.get() + hmod, value_bytes, capacity - hmod);
            std::memcpy(buffer.get(), value_bytes + capacity - hmod, (hmod + size) - capacity);
        } else {
            std::memcpy(buffer.get() + hmod, &value, size);
        }
        h += size;
    }

    template<AllowedType T>
    T get() {
        const auto size = sizeof(T);
        if (h - t < size) {
            throw std::runtime_error("BUFFER UNDERFLOW");
        }
        T value;
        const auto value_bytes = reinterpret_cast<char*>(&value);
        if (const auto tmod = t % capacity; tmod + size > capacity) {
            std::memcpy(value_bytes, buffer.get() + tmod, capacity - tmod);
            std::memcpy(value_bytes + (capacity - tmod), buffer.get(), (tmod + size) - capacity);
        } else {
            std::memcpy(value_bytes, buffer.get() + tmod, size);
        }
        t += size;
        return value;
    }

    void get_bytes(char *value_bytes, const size_t size) {
        if (h - t < size) {
            throw std::runtime_error("BUFFER UNDERFLOW");
        }
        if (const auto tmod = t % capacity; tmod + size > capacity) {
            std::memcpy(value_bytes, buffer.get() + tmod, capacity - tmod);
            std::memcpy(value_bytes + (capacity - tmod), buffer.get(), (tmod + size) - capacity);
        } else {
            std::memcpy(value_bytes, buffer.get() + tmod, size);
        }
        t += size;
    }

    void add_tail(const size_t value) {
        if (h - (t + value) >= capacity) {
            throw std::runtime_error("BUFFER OVERFLOW");
        }
        if (h - (t + value) < 0) {
            throw std::runtime_error("BUFFER UNDERFLOW");
        }
        t += value;
    }

    void add_head(const size_t value) {
        if ((h + value) - t >= capacity) {
            throw std::runtime_error("BUFFER OVERFLOW");
        }
        if ((h + value) - t < 0) {
            throw std::runtime_error("BUFFER OVERFLOW");
        }
        h += value;
    }

    unsigned int remaining() const {
        return h - t;
    }

    void put_bytes(const char* bytes, const size_t size) {
        if ((h + size) - t >= capacity) {
            throw std::runtime_error("BUFFER OVERFLOW");
        }
        if (const auto hmod = h % capacity; hmod + size > capacity) {
            std::memcpy(buffer.get() + hmod, bytes, capacity - hmod);
            std::memcpy(buffer.get(), bytes + capacity - hmod, (hmod + size) - capacity);
        } else {
            std::memcpy(buffer.get() + hmod, bytes, size);
        }
        h += size;
    }

    char* get_data() const {
        return buffer.get();
    }

private:
    std::unique_ptr<char[]> buffer;
    const size_t capacity;
    size_t h = 0;
    size_t t = 0;
};

class BufferTracker {
    bool write_in_progress = false;
    bool connection_in_progress = false;
    int fd = 0;
    RingBuffer read_buffer;
    unsigned int write_buffer_index, read_buffer_index;
    RingBuffer write_buffer;
public:
    BufferTracker(const size_t buffer_capacity, const unsigned int read_buffer_index, const unsigned int write_buffer_index)
        : read_buffer(buffer_capacity),
          write_buffer_index(write_buffer_index),
          read_buffer_index(read_buffer_index),
          write_buffer(buffer_capacity) {
    }

    int get_fd() const {
        return fd;
    }

    void set_fd(const int fd) {
        this->fd = fd;
    }

    bool is_write_in_progress() const {
        return write_in_progress;
    }

    bool is_connection_in_progress() const {
        return connection_in_progress;
    }

    void set_connection_in_progress(const bool value) {
        connection_in_progress = value;
    }

    void set_write_in_progress(const bool value) {
        write_in_progress = value;
    }

    RingBuffer& get_read_buffer() {
        return read_buffer;
    }

    RingBuffer& get_write_buffer() {
        return write_buffer;
    }

    unsigned int get_write_buffer_index() const {
        return write_buffer_index;
    }

    unsigned int get_read_buffer_index() const {
        return read_buffer_index;
    }
};


inline Connection::~Connection() = default;

template<size_t log_size>
Consensus<log_size>::Consensus(
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

    // threads.emplace_back([this] {
    //     try {
    //         while (running.load()) {
    //             const auto current_commit = committed.load();
    //             const auto current_consume = consumed.load();
    //
    //             if (current_consume < current_commit) {
    //                 const auto next = current_consume + 1;
    //                 std::cout << "Consuming log index: " << next << std::endl;
    //                 acks[next % log_size].store(0);
    //                 consumed.store(next);
    //             } else {
    //                 std::this_thread::yield();
    //             }
    //         }
    //     } catch (const std::exception &e) {
    //         std::cerr << "Exception thrown: " << e.what() << std::endl;
    //         shutdown();
    //     }
    // });

    if (io_type == IOType::EPOLL) {
        epoll_provider(algo, instance_configs, total_pipes, total_connections, buffer_size);
    } else if (io_type == IOType::IO_URING) {
        io_uring_provider(algo, instance_configs, total_pipes, total_connections, buffer_size);
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

inline void epoll_write(const int epoll_fd, BufferTracker* tracker) {
    const auto fd = tracker->get_fd();
    auto& write_buffer = tracker->get_write_buffer();
    while (write_buffer.remaining() > 0) {
        const auto [address, write_size] = write_buffer.next_write();
        const auto response = write(fd, reinterpret_cast<void *>(address),write_size);
        if (response == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                epoll_event mod_event{};
                mod_event.events = EPOLLIN | EPOLLOUT | EPOLLET;
                mod_event.data.ptr = static_cast<void*>(tracker);
                if (epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &mod_event) < 0) {
                    close(fd);
                    throw std::runtime_error("epoll_ctl MOD failed to enable EPOLLOUT");
                }
                break;
            }
            close(fd);
            throw std::runtime_error("error writing to socket!");
        }
        write_buffer.add_tail(response);
    }
}

template<size_t log_size>
void Consensus<log_size>::epoll_provider(
    const Algorithm algo,
    const std::vector<InstanceConfig> &instance_configs,
    const unsigned int total_pipes,
    const unsigned int total_connections,
    const size_t buffer_size
) {
    constexpr unsigned int buffer_count = 1000;
    auto& running_ref = running;
    for (int thread_id = 0; thread_id < instance_configs.size(); ++thread_id) {
        threads.emplace_back([&running_ref, total_pipes, &instance_configs, total_connections, thread_id, buffer_size]() {
            try {
                const int num_pipes = total_pipes / instance_configs.size();
                const int num_connections = total_connections / instance_configs.size();
                const auto&[node_id, host_config, peers] = instance_configs[thread_id];
                const auto server_fd = setup_server_socket(host_config.host(), host_config.port());

                std::atomic<unsigned int> ops{};
                std::thread my_thread([&ops, node_id]() {
                    for (;;) {
                        std::this_thread::sleep_for(std::chrono::seconds(1));
                        unsigned int current_ops = ops.exchange(0, std::memory_order_relaxed);
                        std::cout << node_id << "Total ops: " << current_ops<< std::endl;
                    }
                });
                my_thread.detach();

                if (!tune_socket(server_fd)) {
                    close(server_fd);
                    throw std::runtime_error("Failed to tune server socket");
                }
                std::cout << node_id << ": listening on " << host_config.host() << ":" << host_config.port() << " " << server_fd << std::endl;

                const auto epoll_fd = epoll_create1(0);
                if (epoll_fd < 0) {
                    throw std::runtime_error("Failed to create epoll descriptor");
                }
                epoll_event server_event{};
                server_event.events = EPOLLIN | EPOLLET;
                auto* server_tracker = new BufferTracker(buffer_size, 0, 0);
                server_tracker->set_fd(server_fd);
                server_event.data.ptr = static_cast<void*>(server_tracker);

                if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &server_event) < 0) {
                    close(server_fd);
                    throw std::runtime_error("epoll_ctl ADD failed on server socket");
                }

                std::this_thread::sleep_for(std::chrono::seconds(2));

                for (const auto &address: peers) {
                    sockaddr_in server_addr{};
                    server_addr.sin_family = AF_INET;
                    server_addr.sin_port = htons(address.port());
                    inet_pton(AF_INET, address.host().c_str(), &server_addr.sin_addr);

                    for (int i = 0; i < num_connections; i++) {
                        const auto client_fd = socket(AF_INET, SOCK_STREAM, 0);
                        if (client_fd < 0) {
                            close(client_fd);
                            throw std::runtime_error("Failed to create client socket");
                        }
                        std::cout << "Creating connection client fd: " << client_fd << std::endl;

                        if (!tune_socket(client_fd)) {
                            close(client_fd);
                            throw std::runtime_error("Failed to tune client socket");
                        }


                        auto* client_tracker = new BufferTracker(buffer_size, 0, 0);
                        client_tracker->set_fd(client_fd);

                        epoll_event client_event{};
                        client_event.data.ptr = static_cast<void *>(client_tracker);
                        if (const auto result = connect(client_fd, reinterpret_cast<sockaddr *>(&server_addr), sizeof(server_addr)); result == 0) {
                            client_event.events = EPOLLIN | EPOLLET;
                        } else if (result == -1 && errno == EINPROGRESS) {
                            client_tracker->set_connection_in_progress(true);
                            client_event.events = EPOLLOUT | EPOLLET;
                        } else {
                            close(client_fd);
                            throw std::runtime_error("Failed to connect");
                        }

                        if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &client_event) < 0) {
                            close(client_fd);
                            throw std::runtime_error("epoll_ctl ADD failed on connecting socket");
                        }
                    }
                }

                epoll_event epoll_events[128];
                while (true) {
                    const auto n = epoll_wait(epoll_fd, epoll_events, 128, 0);
                    // std::cout << "Epoll events: " << node_id << " - " << n << std::endl;
                    for (int event_id = 0; event_id < n; event_id++) {
                        const auto event = epoll_events[event_id];
                        auto* tracker = static_cast<BufferTracker*>(event.data.ptr);
                        auto fd = tracker->get_fd();

                        if (tracker->get_fd() == server_fd) {
                            while (true) {
                                sockaddr_in client_addr{};
                                socklen_t addr_len = sizeof(client_addr);
                                const auto client_fd = accept(server_fd, reinterpret_cast<sockaddr *>(&client_addr), &addr_len);
                                if (client_fd < 0) {
                                    if (errno == EAGAIN || errno == EWOULDBLOCK) {
                                        break;
                                    }
                                    throw std::runtime_error("error accepting on server!");
                                }

                                if (!tune_socket(client_fd)) {
                                    close(client_fd);
                                    throw std::runtime_error("failed to tune client socket!");
                                }


                                auto* client_tracker = new BufferTracker(buffer_size, 0, 0);
                                client_tracker->set_fd(client_fd);
                                std::cout << node_id << " accepted connection!" << std::endl;
                                epoll_event client_event{};
                                client_event.events = EPOLLIN | EPOLLET;
                                client_event.data.ptr = static_cast<void*>(client_tracker);
                                epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &client_event);

                                if (node_id == 0) {
                                    auto &write_buffer = client_tracker->get_write_buffer();
                                    write_buffer.put<unsigned int>(5);
                                    write_buffer.put<unsigned char>(0);
                                    write_buffer.put<unsigned int>(69);

                                    epoll_write(epoll_fd, client_tracker);
                                    std::cout << "Out of inital write loop" << std::endl;
                                }
                            }
                        } else {
                            if (event.events & EPOLLERR || event.events & EPOLLHUP || event.events & EPOLLRDHUP) {
                                std::cout << "FD: " << fd << "Node: " << node_id << "EPOLLERR? " << (event.events & EPOLLERR)
                                        << " HUP? " << (event.events & EPOLLHUP)
                                        << " RDHUP? " << (event.events & EPOLLRDHUP)
                                        << std::endl;

                                int err = 0;
                                socklen_t len = sizeof(err);
                                if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) == 0) {
                                    if (err != 0) {
                                        std::cerr << "Socket error on fd " << fd << ": " << strerror(err) << std::endl;
                                    }
                                }
                                close(fd);
                                continue;
                            }

                            if (event.events & EPOLLIN) {
                                auto& read_buffer = tracker->get_read_buffer();
                                while (running_ref.load()) {
                                    const auto [address, read_size] = read_buffer.next_read();
                                    if (read_size <= 0) {
                                        throw std::runtime_error("BUFFER OUT OF SPACE ON READ!");
                                    }
                                    const auto bytes_read = read(fd, reinterpret_cast<void*>(address), read_size);
                                    if (bytes_read == -1) {
                                        if (errno == EAGAIN || errno == EWOULDBLOCK) {
                                             break;
                                        }
                                        close(fd);
                                        throw std::runtime_error("error reading on socket!");
                                    }

                                    if (bytes_read == 0) {
                                        close(fd);
                                        break;
                                    }
                                    read_buffer.add_head(bytes_read);

                                    while (true) {
                                        if (read_buffer.remaining() < 4) {
                                            break;
                                        }

                                        if (const auto payload_size = read_buffer.get<unsigned int>(); read_buffer.remaining() >= payload_size) {
                                            read_buffer.add_tail(payload_size);
                                            ops.fetch_add(1, std::memory_order_relaxed);
                                            auto &write_buffer = tracker->get_write_buffer();
                                            write_buffer.put<unsigned int>(5);
                                            write_buffer.put<unsigned char>(0);
                                            write_buffer.put<unsigned int>(69);

                                            epoll_write(epoll_fd, tracker);
                                        } else {
                                            read_buffer.add_tail(-4);
                                            break;
                                        }
                                    }
                                }
                            }

                            if (event.events & EPOLLOUT) {
                                std::cout << "Epoll out?" << std::endl;
                                if (tracker->is_connection_in_progress()) {
                                    int err = 0;
                                    socklen_t len = sizeof(err);
                                    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0 || err != 0) {
                                        close(fd);
                                        throw std::runtime_error("failed to connect socket error!");
                                    }

                                    epoll_event next_event{};
                                    next_event.events = EPOLLIN | EPOLLET;
                                    next_event.data.ptr = event.data.ptr;

                                    std::cout << node_id << " completed connection!" << std::endl;

                                    if (epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &next_event) < 0) {
                                        close(fd);
                                        throw std::runtime_error("epoll_ctl MOD failed after connect");
                                    }

                                    tracker->set_connection_in_progress(false);
                                } else {
                                    std::cout << "Are we writing out here?" << std::endl;
                                    auto &write_buffer = tracker->get_write_buffer();
                                    while (write_buffer.remaining() > 0) {
                                        const auto [address, write_size] = write_buffer.next_write();
                                        const auto response = write(fd, reinterpret_cast<void *>(address), write_size);
                                        if (response == -1) {
                                            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                                                break;
                                            }
                                            close(fd);
                                            throw std::runtime_error("error writing to socket!");
                                        }
                                        write_buffer.add_tail(response);
                                    }

                                    if (write_buffer.remaining() == 0) {
                                        epoll_event next_event{};
                                        next_event.events = EPOLLIN | EPOLLET;
                                        next_event.data.ptr = event.data.ptr;
                                        if (epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &next_event) < 0) {
                                            close(fd);
                                            throw std::runtime_error("epoll_ctl MOD failed after write");
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            } catch (std::exception &e) {
                std::cerr << "Exception thrown: " << e.what() << std::endl;
            }
        });
    }
}

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

    std::vector<iovec> io_vecs;

    int node_id = 0;

    static constexpr size_t buffer_count = 1000;

    std::vector<std::unique_ptr<BufferTracker>> trackers;

    void initialize(const size_t buffer_size, const int sq_entries, const int node_id) {
        this->node_id = node_id;
        params = std::make_unique<io_uring_params>();
        std::memset(params.get(), 0, sizeof(io_uring_params));
        params->flags |= IORING_SETUP_SQPOLL;
        // | IORING_SETUP_SQ_AFF;
        params->sq_thread_idle = 1000000;
        // params->sq_thread_cpu = node_id + 2;

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


        trackers.resize(buffer_count);
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
        // delete params;

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
        const int server_fd,
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
        const int client_fd,
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
        const int fd,
        const unsigned int conn_index,
        BufferTracker &tracker
    ) const {
        if (!tracker.is_write_in_progress()) {
            auto [address, size] = tracker.get_write_buffer().next_write();
            tracker.set_write_in_progress(true);
            const auto [sqe, tail] = get_sqe();
            sqe->opcode = IORING_OP_WRITE_FIXED;
            sqe->fd = static_cast<__s32>(conn_index);
            sqe->buf_index = tracker.get_write_buffer_index();
            sqe->addr = address;
            sqe->off = 0;
            sqe->len = size;
            sqe->flags = IOSQE_FIXED_FILE;
            sqe->user_data = pack_fd_index_opcode(fd, conn_index, IORING_OP_WRITE_FIXED);
            sq_tail->store(tail + 1, std::memory_order_release);
        }
    }

    void submit_read(
        const int fd,
        const int conn_index,
        BufferTracker &tracker
    ) const {
        auto [address, size] = tracker.get_read_buffer().next_read();
        const auto [sqe, tail] = get_sqe();
        sqe->opcode = IORING_OP_READ_FIXED;
        sqe->fd = conn_index;
        sqe->buf_index = tracker.get_read_buffer_index();
        sqe->addr = address;
        sqe->len = size;
        sqe->off = 0;
        sqe->flags = IOSQE_FIXED_FILE;
        sqe->user_data = pack_fd_index_opcode(fd, conn_index, IORING_OP_READ_FIXED);
        sq_tail->store(tail + 1, std::memory_order_release);
    }

    void on_write(
        const int fd,
        const int conn_index,
        const int response
    ) const {
        if (response <= 0) {
            throw std::runtime_error("ERROR WRITING TO SOCKET");
        }
        auto &tracker = *trackers[conn_index];
        auto &write_buffer = tracker.get_write_buffer();
        tracker.set_write_in_progress(false);
        write_buffer.add_tail(response);
        if (write_buffer.remaining() > 0) {
            submit_write(fd, conn_index, tracker);
        }
    }

    void on_read(
        const int fd,
        const int conn_index,
        const int response
    ) {
        if (response < 0) {
            throw std::runtime_error("Error reading from socket!");
        }
        if (response == 0) {
            throw std::runtime_error("EOF reached");
        }

        auto &tracker = *trackers[conn_index];
        auto& read_buffer = tracker.get_read_buffer();
        read_buffer.add_head(response);
        while (true) {
            if (read_buffer.remaining() >= 4) {
                if (const auto amount = read_buffer.get<unsigned int>(); read_buffer.remaining() >= amount) {
                    std::cout << "Got packet with size: " << amount << std::endl;
                    // submit_write(fd, conn_index, tracker);
                } else {
                    read_buffer.add_tail(-4);
                    break;
                }
            } else break;
        }

        submit_read(fd, conn_index, tracker);
    }

    void on_connect(
        const int fd,
        const int conn_index,
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
        const int fd,
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
        auto &tracker = *trackers[client_index];
        submit_read(fd, client_index, tracker);
    }
};

template<size_t log_size>
void Consensus<log_size>::io_uring_provider(
    const Algorithm algo,
    const std::vector<InstanceConfig> &instance_configs,
    const unsigned int total_pipes,
    const unsigned int total_connections,
    const size_t buffer_size
) {
    auto& running_ref = running;

    for (int thread_id = 0; thread_id < static_cast<int>(instance_configs.size()); ++thread_id) {
        threads.emplace_back(
            [thread_id, &instance_configs, total_pipes, total_connections, buffer_size, &running_ref]() {
                try {
                    const int num_pipes = total_pipes / instance_configs.size();
                    const int num_connections = total_connections / instance_configs.size();
                    const auto&[node_id, host_config, peers] = instance_configs[thread_id];
                    const auto context = std::make_shared<IoUringContext>();
                    context->initialize(buffer_size, 1024, node_id);

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
                            if (client_fd < 0) {
                                throw std::runtime_error("Error creating client");
                            }

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

