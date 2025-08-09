#pragma once
#include "consensus.h"

inline Connection::~Connection() {
    std::cout << "Deleting connection?" << std::endl;
    delete[] read_buffer;
    delete[] write_buffer;
}

template<size_t log_size>
Consensus<log_size>::Consensus(
    const IOType io_type,
    const Algorithm algo,
    const std::vector<InstanceConfig> &instance_configs,
    const int total_pipes,
    const int total_connections,
    const size_t buffer_size
) {
    for (size_t i = 0; i < log_size; ++i) {
        std::atomic_init(&acks_[i], 0);
        log_[i] = std::make_unique<char[]>(buffer_size);
    }

    threads_.emplace_back([this] {
        try {
            while (running_.load()) {
                int committed = committed_.load();
                int consumed = consumed_.load();

                if (consumed < committed) {
                    std::cout << "Consuming log index: " << consumed << std::endl;
                    acks_[consumed % log_size].store(0);
                    consumed_.store(consumed + 1);
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
        epoll_provider(algo, instance_configs, total_pipes, total_connections);
    } else if (io_type == IOType::IO_URING) {
        io_uring_provider(algo, instance_configs, total_pipes, total_connections);
    }
}

template<size_t log_size>
void Consensus<log_size>::shutdown() {
    running_.store(false);
    for (auto &t : threads_) {
        if (t.joinable()) {
            t.join();
        }
    }
}

template<size_t log_size>
Consensus<log_size>::~Consensus() {
    shutdown();
}

template<size_t log_size>
void Consensus<log_size>::epoll_provider(
    const Algorithm algo,
    const std::vector<InstanceConfig> &instance_configs,
    const int total_pipes,
    const int total_connections
) {
    std::cout << "Epoll provider!" << std::endl;
}


template<size_t log_size>
void Consensus<log_size>::io_uring_provider(
    const Algorithm algo,
    const std::vector<InstanceConfig> &instance_configs,
    const int total_pipes,
    const int total_connections
) {

}