#pragma once
#include <arpa/inet.h>
#include <netinet/in.h>

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




template<size_t log_size>
void Consensus<log_size>::epoll_provider(
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

                         std::cout << "Connecting to user: " << host << ":" << port << " " << client_fd << std::endl;

                         const auto connection = new Connection{read_buffer, write_buffer, 0, 0, true};

                         connections.push_back(new Connection{read_buffer, write_buffer, 0, 0, true});
                         epoll_event client_event{};
                         client_event.events = EPOLLIN | EPOLLOUT | EPOLLET;
                         client_event.data.u64 = pack_fd_and_index(client_fd, connection);
                         epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &client_event);
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
    const Algorithm algo,
    const std::vector<InstanceConfig> &instance_configs,
    const unsigned int total_pipes,
    const unsigned int total_connections,
    const size_t buffer_size
) {

}