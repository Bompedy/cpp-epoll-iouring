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


std::atomic RUNNING { true };

class Address {
    std::string host_;
    unsigned short port_;

    bool operator==(const Address& other) const {
        return host_ == other.host_ && port_ == other.port_;
    }
public:
    Address(std::string host, const unsigned short port) : host_(std::move(host)), port_(port) {}
    [[nodiscard]] const std::string& host() const { return host_; }
    [[nodiscard]] unsigned short port() const { return port_; }
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

    if (!tune_socket(server_fd, 1024 * 4 * 4)) {
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

    std::string str(val);
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

void node(
    const unsigned char node_id,
    const bool is_leader,
    const std::vector<Address> &peers,
    std::vector<std::thread> &workers

) {
    workers.emplace_back([&peers, node_id, is_leader]() {
        try {
            std::cout << "Okay inside of here with node: " << node_id << std::endl;
            int slot = 0, consumed = 0, committed = 0;
            constexpr unsigned int log_size = 10000;
            char* log[log_size];
            char* storage[log_size];
            unsigned char acks[log_size];


            constexpr auto quorum = (peers.size() / 2) + 1;
            const auto &address = peers[node_id];
            const auto server_fd = setup_server_socket(address.host(), address.port());

            sockaddr_in client_addr{};
            auto* client_sockaddr = reinterpret_cast<sockaddr*>(&client_addr);
            socklen_t cli_addr_len = sizeof(client_addr);
            constexpr auto buffer_size = 1000000;
            char buffer[buffer_size];

            while (RUNNING.load(std::memory_order_relaxed)) {
                while (committed > consumed) {
                    // grab log index, check if read
                    const auto is_read = false;
                    if (is_read) {
                        // write back to client
                    }
                    consumed++;
                }

                if (const auto size = recvfrom(server_fd, buffer, buffer_size, 0, client_sockaddr, &cli_addr_len);
                    size > 0) {
                    const auto op = buffer[0];
                    switch (op) {
                        case OP_CLIENT_REQUEST: {
                            if (const auto type = buffer[1]; type == REQUEST_READ) {
                                // do read stuff
                            } else {
                                // do write stuff
                            }

                            // propose
                            break;
                        }

                        case OP_PROPOSE: {
                            int proposed_slot;
                            std::memcpy(&proposed_slot, &buffer[1], sizeof(int));
                            // fill log ack back
                            break;
                        }
                        case OP_ACK: {
                            int acked_slot;
                            std::memcpy(&acked_slot, &buffer[1], sizeof(int));
                            acks[acked_slot] += 1;
                            const auto current_commit = committed;
                            while (acks[committed+1] >= quorum) {
                                ++committed;
                            }
                            if (current_commit != committed) {
                                // write out commit packet
                            }
                            break;
                        }

                        case OP_COMMIT: {
                            int next_commit;
                            std::memcpy(&next_commit, &buffer[1], sizeof(int));
                            committed = next_commit;
                            break;
                        }

                        default: { throw std::runtime_error("Invalid operation"); }
                    }
                }
            }

            std::cout << "Broke out?" << std::endl;
        } catch (std::exception &e) {
            std::cerr << e.what() << std::endl;
        }
    });
}

void client(
    const Address& leader,
    const unsigned int connections,
    const unsigned int ops,
    const unsigned int data_size,
    std::vector<std::thread> &workers
) {
    const auto ops_per_conn = ops / connections;
    for (unsigned int i = 0; i < connections; i++) {
        workers.emplace_back([&leader, data_size, ops_per_conn]() {
            auto completed_ops = 0;
            const auto client_fd = socket(AF_INET, SOCK_DGRAM, 0);
            sockaddr_in cli_addr{};
            cli_addr.sin_family = AF_INET;
            cli_addr.sin_port = htons(leader.port());
            socklen_t addr_len = sizeof(cli_addr);
            auto* client_sockaddr = reinterpret_cast<sockaddr*>(&cli_addr);
            char buffer[data_size];

            if (inet_pton(AF_INET, leader.host().c_str(), &cli_addr.sin_addr) <= 0) {
                close(client_fd);
                throw std::runtime_error("Invalid address");
            }

            bool wrote = false;
            while (RUNNING.load(std::memory_order_relaxed)) {
                // write out one packet and wait for response
                if (!wrote) {

                    wrote = true;
                }
                if (const auto size = recvfrom(client_fd, buffer, data_size, 0, client_sockaddr, &addr_len); size > 0) {
                    if (buffer[0] == OP_CLIENT_RESPONSE) {
                        if (++completed_ops >= ops_per_conn) {
                            break;
                        }
                        wrote = false;
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
        std::cout << "Running it" << std::endl;
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
        //
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
        // client(Address { "127.0.0.1", 6969 }, 1, 1, 1, workers);

        while (RUNNING.load()) {
            pause();
        }

        for (auto& worker : workers) {
            worker.join();
        }

        std::cout << "Shutting down..." << std::endl;
    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
    }
}