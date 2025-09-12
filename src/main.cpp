#include <atomic>
#include <csignal>
#include <cstring>
#include <iostream>
#include <sstream>
#include <vector>
#include <arpa/inet.h>
#include <bits/std_thread.h>
#include <bits/this_thread_sleep.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <unordered_map>


std::atomic RUNNING { true };

class Address {
    sockaddr_storage storage_{};     // Generic storage for sockaddr
    sockaddr* sock_;               // Pointer to casted sockaddr_in
    std::string host_;
    unsigned short port_;

public:
    Address(std::string host, const unsigned short port) : host_(std::move(host)), port_(port) {
        std::memset(&storage_, 0, sizeof(storage_));

        // Fill in IPv4 sockaddr_in
        auto* addr_in = reinterpret_cast<sockaddr_in*>(&storage_);
        addr_in->sin_family = AF_INET;
        addr_in->sin_port = htons(port_);
        if (inet_pton(AF_INET, host_.c_str(), &addr_in->sin_addr) <= 0) {
            throw std::invalid_argument("Invalid IPv4 address: " + host_);
        }

        sock_ = reinterpret_cast<sockaddr*>(&storage_);
    }

    // Accessors
    [[nodiscard]] const std::string& host() const { return host_; }
    [[nodiscard]] unsigned short port() const { return port_; }

    // Equality
    bool operator==(const Address& other) const {
        return host_ == other.host_ && port_ == other.port_;
    }

    // Return sockaddr* directly (no casting needed)
    [[nodiscard]] sockaddr* sockaddr_ptr() const { return sock_; }
    [[nodiscard]] socklen_t sockaddr_len() const { return sizeof(sockaddr_in); }
};
bool tune_socket(
    const int fd,
    const unsigned int buffer_size
    ) {
    const auto flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) {
        perror("fcntl F_GETFL");
        return false;
    }
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        perror("fcntl F_SETFL O_NONBLOCK");
        return false;
    }

    // Set send buffer size
    if (setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &buffer_size, sizeof(buffer_size)) < 0) {
        perror("setsockopt SO_SNDBUF failed");
        return false;
    }

    // Set receive buffer size
    if (setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &buffer_size, sizeof(buffer_size)) < 0) {
        perror("setsockopt SO_RCVBUF failed");
        return false;
    }

    return true;
}

int setup_server_socket(const std::string& address, const unsigned short port) {
    constexpr int opt = 1;
    const auto server_fd = socket(AF_INET, SOCK_DGRAM, 0);

    if (server_fd < 0) {
        throw std::runtime_error("server socket failed: " + std::string(std::strerror(errno)));
    }

    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt)) < 0) {
        close(server_fd);
        throw std::runtime_error("Failed to set reuseport to socket");
    }

    const auto flags = fcntl(server_fd, F_GETFL, 0);
    if (flags == -1) {
        perror("fcntl F_GETFL");
        return false;
    }

    if (!tune_socket(server_fd, 1024 * 4 * 1024)) {
        close(server_fd);
        throw std::runtime_error("Failed to create tune socket");
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

    if (inet_pton(AF_INET, address.c_str(), &addr.sin_addr) <= 0) {
        close(server_fd);
        throw std::runtime_error("Invalid address: " + address);
    }

    if (bind(server_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
        close(server_fd);
        throw std::runtime_error("Failed to bind to port " + std::to_string(port));
    }

    return server_fd;
}

void shutdown(const int signum) {
    std::cout << "Received signal: " << signum << std::endl;
    RUNNING.store(false);
}

unsigned int getEnvUInt(const char* name) {
    const char* val = std::getenv(name);
    if (!val) throw std::runtime_error("Environment variable " + std::string(name) + " is not set");

    try {
        return static_cast<unsigned int>(std::stoul(val));
    } catch (...) {
        throw std::invalid_argument(std::string("Invalid uint for env var: ") + name);
    }
}

std::vector<Address> getEnvPeers(const char* env_var_name) {
    const char* val = std::getenv(env_var_name);
    if (!val) {
        throw std::runtime_error(std::string("Missing required environment variable: ") + env_var_name);
    }

    const std::string str(val);
    std::vector<Address> peers;
    std::stringstream ss(str);
    std::string item;

    while (std::getline(ss, item, ',')) {
        auto pos = item.find(':');
        if (pos == std::string::npos || pos == 0 || pos == item.length() - 1) {
            throw std::invalid_argument("Invalid peer address format: " + item);
        }

        std::string host = item.substr(0, pos);
        unsigned short port = static_cast<unsigned short>(std::stoi(item.substr(pos + 1)));

        peers.emplace_back(host, port);
    }

    return peers;
}

Address getEnvAddress(const char* env_var_name) {
    const char* val = std::getenv(env_var_name);
    if (!val) {
        throw std::runtime_error(std::string("Missing required environment variable: ") + env_var_name);
    }

    std::string str(val);
    const auto pos = str.find(':');
    if (pos == std::string::npos || pos == 0 || pos == str.length() - 1) {
        throw std::invalid_argument(std::string("Invalid address format for ") + env_var_name + ": " + str);
    }

    std::string host = str.substr(0, pos);
    unsigned short port = static_cast<unsigned short>(std::stoi(str.substr(pos + 1)));

    return Address{host, port};
}

constexpr unsigned char OP_CLIENT_REQUEST = 0;
constexpr unsigned char OP_CLIENT_RESPONSE = 1;
constexpr unsigned char OP_PROPOSE = 2;
constexpr unsigned char OP_ACK = 3;
constexpr unsigned char OP_COMMIT = 4;

constexpr unsigned char REQUEST_WRITE = 0;
constexpr unsigned char REQUEST_READ = 1;

inline void broadcast(
    const int fd,
    const unsigned char node_id,
    const std::vector<Address> &peers,
    const char* buffer,
    const unsigned int buffer_size
) {
    for (int i = 0; i < peers.size(); ++i) {
        if (i != node_id) {
            if (sendto(fd, buffer, buffer_size, 0, peers[i].sockaddr_ptr(), peers[i].sockaddr_len()) <= 0) {
                throw std::runtime_error("Failed to send message to node " + std::to_string(node_id));
            }
        }
    }
}

#include <vector>
#include <stack>
#include <stdexcept>
#include <cstddef>
#include <memory>

#include <vector>
#include <stdexcept>
#include <cstddef>

#include <vector>
#include <cstddef>
#include <stdexcept>

#include <vector>
#include <mutex>

struct BufferPool {
    std::vector<char*> free_buffers;
    size_t buffer_size;
    std::mutex mutex;

    BufferPool(size_t buffer_count, size_t buffer_size)
        : buffer_size(buffer_size)
    {
        for (size_t i = 0; i < buffer_count; ++i) {
            free_buffers.push_back(new char[buffer_size]);
        }
    }

    ~BufferPool() {
        for (char* buf : free_buffers) {
            delete[] buf;
        }
    }

    char* acquire() {
        if (free_buffers.empty()) {
            return new char[buffer_size];
        }
        char* buf = free_buffers.back();
        free_buffers.pop_back();
        return buf;
    }

    void release(char* buffer) {
        free_buffers.push_back(buffer);
    }

    size_t available() {
        return free_buffers.size();
    }
};




void node(
    const unsigned char node_id,
    const bool is_leader,
    const std::vector<Address> &peers,
    std::vector<std::thread> &workers

) {
    workers.emplace_back([&peers, node_id, is_leader, &workers]() {
        try {
            constexpr auto buffer_size = 100;
            constexpr unsigned int log_size = 1100000;
            int slot = 0, consumed = 0;

            std::atomic<unsigned int> committed(0);
            auto log = new char*[log_size];
            // std::unordered_map<std::string, char*> storage{};
            auto* acks = new std::atomic<unsigned char>[log_size];
            for (unsigned int i = 0; i < log_size; ++i) {
                acks[i].store(0);
            }

            const auto quorum = (peers.size() / 2) + 1;
            const auto &address = peers[node_id];
            const auto server_fd = setup_server_socket(address.host(), address.port());

            workers.emplace_back([server_fd, &committed, &consumed, log, is_leader, node_id]() {
                while (RUNNING.load(std::memory_order_relaxed)) {
                    if (committed.load(std::memory_order_relaxed) > consumed) {
                        const auto data = log[consumed % log_size];
                        if (data == nullptr) {
                            if (is_leader) {
                                throw std::runtime_error("such a bad problem");
                            }

                            // break;
                            consumed++;
                            // std::cout << "Stuck trying to consume slot: " << consumed << std::endl;
                            continue;
                        }
                        const auto is_read = data[21] == REQUEST_READ;
                        const auto is_write = data[21] == REQUEST_WRITE;

                        if (is_write) {
                            // fill kv
                        }
                        if (is_leader) {
                            sockaddr sender_addr{};
                            std::memcpy(&sender_addr, &data[5], sizeof(sockaddr_in));
                            if (is_write) {
                                // std::cout << "Gonna write back to client for slot: " << consumed << std::endl;
                                data[0] = OP_CLIENT_RESPONSE;
                                if (sendto(server_fd, data, 1, 0, &sender_addr, sizeof(sockaddr)) <= 0) {
                                    throw std::runtime_error(
                                        "Failed to send message to node " + std::to_string(node_id));
                                }
                            } else if (is_read) {
                            } else throw std::invalid_argument("Invalid leader address format");
                        }

                        // acks[consumed % log_size] = 0;
                        // pool->release(data);
                        consumed++;
                    } else std::this_thread::yield();
                }
            });

            if (is_leader) {
                workers.emplace_back([&slot, acks, log, node_id, &peers, log_size, buffer_size, server_fd]() {
                    const auto request_fd = setup_server_socket("127.0.0.1", 7069);
                    auto pool = new BufferPool(log_size, buffer_size);
                    sockaddr_in client_addr{};
                    auto *client_sockaddr = reinterpret_cast<sockaddr *>(&client_addr);
                    socklen_t cli_addr_len = sizeof(client_addr);
                    while (RUNNING.load(std::memory_order_relaxed)) {
                        const auto buffer = pool->acquire();
                        if (const auto size = recvfrom(request_fd, buffer, buffer_size, 0, client_sockaddr,
                                                       &cli_addr_len); size > 0) {
                            std::cout << "Got " << size << " bytes from node " << node_id << std::endl;
                            if (buffer[0] == OP_CLIENT_REQUEST) {
                                std::cout << "Got client request" << std::endl;
                                const auto next_slot = slot++;
                                if (acks[next_slot % log_size] != 0) {
                                    throw std::runtime_error(
                                        "OUT OF LOG SPACE AT INDEX: " + std::to_string(next_slot) + " " +
                                        std::to_string(acks[next_slot % log_size]));
                                }
                                acks[next_slot % log_size].store(1);
                                std::memcpy(&buffer[1], &next_slot, sizeof(int));
                                std::memcpy(&buffer[5], &client_addr, cli_addr_len);
                                log[next_slot % log_size] = buffer;
                                buffer[0] = OP_PROPOSE;
                                broadcast(request_fd, node_id, peers, buffer, size);
                            } else {
                                throw std::invalid_argument(
                                    "Invalid op on client request: " + std::to_string(buffer[0]));
                            }
                        } else {
                            pool->release(buffer);
                        }
                    }

                    delete pool;
                    close(request_fd);
                });
            }

            sockaddr_in client_addr{};
            auto* client_sockaddr = reinterpret_cast<sockaddr*>(&client_addr);
            socklen_t cli_addr_len = sizeof(client_addr);
            const auto pool = new BufferPool(log_size*2, buffer_size);
            while (RUNNING.load(std::memory_order_relaxed)) {
                const auto buffer = pool->acquire();
                if (const auto size = recvfrom(server_fd, buffer, buffer_size, 0, client_sockaddr, &cli_addr_len); size > 0) {
                    switch (const auto op = buffer[0]) {
                        case OP_PROPOSE: {
                            std::cout << "Got proposal request on node: " << node_id << std::endl;
                            buffer[0] = OP_ACK;
                            int proposed_slot;
                            std::memcpy(&proposed_slot, &buffer[1], sizeof(int));
                            log[proposed_slot % log_size] = buffer;

                            if (sendto(server_fd, buffer, 5, 0, peers[0].sockaddr_ptr(), peers[0].sockaddr_len()) <= 0) {
                                 throw std::runtime_error("Failed to send message to node " + std::to_string(node_id));
                            }
                            break;
                        }

                        case OP_ACK: {
                            int acked_slot;
                            std::cout << "Got ack back for slot: " << acked_slot << std::endl;
                            std::memcpy(&acked_slot, &buffer[1], sizeof(int));
                            acks[acked_slot % log_size] += 1;

                            const auto before_commit = committed.load(std::memory_order_relaxed);
                            auto current_commit = before_commit;
                            while (acks[current_commit % log_size].load() >= quorum) {
                                ++current_commit;
                            }

                            if (before_commit != current_commit) {
                                std::cout << "Moving commit up to: " << current_commit << std::endl;
                                committed.store(current_commit, std::memory_order_relaxed);
                                buffer[0] = OP_COMMIT;
                                std::memcpy(&buffer[1], &current_commit, sizeof(int));
                                broadcast(server_fd, node_id, peers, buffer, 5);
                            }

                            pool->release(buffer);
                            break;
                        }

                        case OP_COMMIT: {
                            int next_commit;
                            std::memcpy(&next_commit, &buffer[1], sizeof(int));
                            if (next_commit < committed) {
                                throw std::runtime_error("NEXT COMMIT SMALLER THAN COMMITTED");
                            }
                            committed.store(next_commit, std::memory_order_relaxed);
                            pool->release(buffer);
                            break;
                        }

                        default: { throw std::runtime_error("Invalid operation"); }
                    }
                } else {
                    pool->release(buffer);
                }
            }
            delete[] log;
            delete[] acks;
            delete pool;
            close(server_fd);
            std::cout << "Broke out?" << std::endl;
        } catch (std::exception &e) {
            std::cerr << e.what() << std::endl;
        }
    });
}

long time_millis() {
    auto now = std::chrono::system_clock::now();
    auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(
            now.time_since_epoch()
    ).count();
    return millis;
}

void client(
    const Address& leader,
    const unsigned int connections,
    const unsigned int ops,
    const unsigned int data_size,
    std::vector<std::thread> &workers
) {
    const auto ops_per_conn = ops / connections;
    auto completed_connections = std::make_shared<std::atomic<int>>(0);
    auto send_times = std::make_shared<std::vector<long>>();
    auto start = std::make_shared<long>(0);

    workers.emplace_back([completed_connections, connections, ops, data_size, start]() {
        while (completed_connections->load() != connections) {
            std::this_thread::yield();
        }
        auto end = time_millis();
        auto seconds = (float)(end-*start) / 1e3f;
        auto mbps = (((float) ops * (float) (data_size * 8)) / 1e6f) / seconds;
        auto ops_per_second = (float) ops / seconds;
        std::cout << "Update - Count(" << ops << ") OPS(" << ops_per_second << ") Seconds(" << seconds << ") Throughput(" << mbps << " Mbps)" << std::endl;
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    // std::cout << "Started" << std::endl;
    *start = time_millis();
    for (unsigned int i = 0; i < connections; i++) {
        workers.emplace_back([&leader, completed_connections, data_size, ops_per_conn]() {
            // std::cout << "Starting thread" << std::endl;
            auto completed_ops = 0;
            const auto client_fd = socket(AF_INET, SOCK_DGRAM, 0);
            if (!tune_socket(client_fd, 1024 * 4 * 1024)) {
                throw std::runtime_error("Failed to create socket");
            }

            sockaddr_in cli_addr{};
            cli_addr.sin_family = AF_INET;
            cli_addr.sin_port = htons(leader.port());
            socklen_t addr_len = sizeof(cli_addr);
            auto* client_sockaddr = reinterpret_cast<sockaddr*>(&cli_addr);
            char write_buffer[data_size+100];
            char read_buffer[data_size+100];

            if (inet_pton(AF_INET, leader.host().c_str(), &cli_addr.sin_addr) <= 0) {
                close(client_fd);
                throw std::runtime_error("Invalid address");
            }

            write_buffer[0] = OP_CLIENT_REQUEST;

            bool should_send = true;
            bool is_write = true;

            while (RUNNING.load(std::memory_order_relaxed)) {
                if (should_send) {
                    if (is_write) {
                        write_buffer[21] = REQUEST_WRITE;
                        // key, value
                    } else {
                        write_buffer[21] = REQUEST_READ;
                        // key
                    }
                    // std::cout << "Sending out client request" << std::endl;
                    if (sendto(client_fd, write_buffer, data_size+22, 0, leader.sockaddr_ptr(), leader.sockaddr_len()) <= 0) {
                        throw std::runtime_error("Failed to send message from client to leader");
                    }
                    should_send = false;
                }
                if (const auto size = recvfrom(client_fd, read_buffer, data_size+100, 0, client_sockaddr, &addr_len); size > 0) {
                    if (read_buffer[0] == OP_CLIENT_RESPONSE) {
                        std::cout << "Got client response" << std::endl;
                        ++completed_ops;
                        if (completed_ops % 10000 == 0) {
                            std::cout << completed_ops << std::endl;
                        }
                        if (completed_ops >= ops_per_conn) {
                            std::cout << "Incrementing completed: " << completed_ops << std::endl;
                            completed_connections->fetch_add(1);
                            break;
                        }
                        should_send = true;

                    } else {
                        throw std::runtime_error("Invalid client response");
                    }
                }
            }
        });
    }
}

int main() {
    try {
        // std::cout << "a Running it" << std::endl;
        struct sigaction action {};
        action.sa_handler = shutdown;
        sigemptyset(&action.sa_mask);
        action.sa_flags = 0;
        sigaction(SIGINT,  &action, nullptr);
        sigaction(SIGTERM, &action, nullptr);
        sigaction(SIGQUIT, &action, nullptr);
        sigaction(SIGHUP,  &action, nullptr);

        std::vector<std::thread> workers;

        // if (const char* is_client_env = std::getenv("IS_CLIENT"); is_client_env && (std::string(is_client_env) == "1" || std::string(is_client_env) =="true")) {
        //     // const auto leader = getEnvAddress("LEADER_ADDRESS");
        //     // const auto connections = getEnvUInt("CONNECTIONS");
        //     // const auto ops = getEnvUInt("OPS");
        //     // const auto data_size = getEnvUInt("DATA_SIZE");
        // } else {
        //     // const auto node_id = getEnvUInt("NODE_ID");
        //     // const auto is_leader = getEnvUInt("IS_LEADER");
        //     // const auto connections = getEnvUInt("CONNECTIONS");
        //     // const auto address = getEnvAddress("ADDRESS");
        //     // const auto peers = getEnvPeers("PEERS");
        //     // node(node_id, is_leader, address, peers, workers);
        // }

        std::vector<Address> peers = {
            Address { "127.0.0.1", 6969},
            Address { "127.0.0.1", 6970},
            Address { "127.0.0.1", 6971}
        };

        node(0, true, peers, workers);
        node(1, false, peers, workers);
        node(2, false, peers, workers);

        std::this_thread::sleep_for(std::chrono::seconds(1));
        client(Address { "127.0.0.1", 7069 }, 1, 1, 1, workers);

        while (RUNNING.load()) {
            pause();
        }

        for (auto& worker : workers) {
            worker.join();
        }
        //

        std::cout << "Shutting down..." << std::endl;
    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
    }
}