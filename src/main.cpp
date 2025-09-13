#include <csignal>
#include <iostream>
#include <sstream>
#include <thread>
#include "shared.h"
#include "node.h"
#include "client.h"

unsigned int getEnvUInt(const char *name);
std::vector<Address> getEnvPeers(const char *env_var_name);
Address getEnvAddress(const char *env_var_name);

void shutdown(const int signum) {
    std::cout << "Received signal: " << signum << std::endl;
    RUNNING.store(false);
}

int main() {
    try {
        sigset_t sigset;
        sigemptyset(&sigset);
        sigaddset(&sigset, SIGINT);
        sigaddset(&sigset, SIGTERM);
        sigaddset(&sigset, SIGQUIT);
        sigaddset(&sigset, SIGHUP);

        // Block in all threads
        pthread_sigmask(SIG_BLOCK, &sigset, nullptr);

        std::vector<std::thread> workers;
        //
        // std::vector<Address> peers = {
        //     Address{"127.0.0.1", 6969},
        //     Address{"127.0.0.1", 6970},
        //     Address{"127.0.0.1", 6971}
        // };

        if (getEnvUInt("IS_CLIENT")) {
            const auto leader_address = getEnvAddress("LEADER_ADDRESS");
            const auto host_address = getEnvAddress("HOST_ADDRESS");
            const auto connections = getEnvUInt("CONNECTIONS");
            const auto ops = getEnvUInt("OPS");
            const auto data_size = getEnvUInt("DATA_SIZE");
            std::cout << "Starting client with configuration:\n"
                 << "  Leader Address : " << leader_address.host() << ":" << leader_address.port() << "\n"
                << "  Host Address : " << host_address.host() << ":" << host_address.port() << "\n"
                 << "  Connections    : " << connections << "\n"
                 << "  Ops            : " << ops << "\n"
                 << "  Data Size      : " << data_size << std::endl;

            client(host_address, leader_address, connections, ops, data_size, workers);
        } else {
            const unsigned char node_id = getEnvUInt("NODE_ID");
            const unsigned char leader_id = getEnvUInt("LEADER_ID");
            const auto buffer_size = getEnvUInt("BUFFER_SIZE");
            const auto log_size = getEnvUInt("LOG_SIZE");
            const auto peers = getEnvPeers("PEERS");
            const auto client_listener = getEnvAddress("CLIENT_LISTENER");
            std::cout << "Starting node with configuration:\n"
                         << "  Node ID        : " << static_cast<int>(node_id) << "\n"
                         << "  Leader ID      : " << static_cast<int>(leader_id) << "\n"
                         << "  Buffer Size    : " << buffer_size << "\n"
                         << "  Log Size       : " << log_size << "\n"
                        << "  Client Address : " << client_listener.host() << ":" << client_listener.port() << "\n"
                         << "  Peers:\n";

            for (const auto& peer : peers) {
                std::cout << "    - " << peer.host() << ":" << peer.port() << "\n";
            }
            auto node_ptr = std::make_shared<Node>(
                  node_id, leader_id, client_listener, peers, buffer_size, log_size
              );
            node(node_ptr, workers);
        }

        // int buffer_size = 11000;
        // int log_size = 150000;
        // Node node0{ 0, 0, peers, buffer_size, log_size };
        // Node node1{ 1, 0, peers, buffer_size, log_size };
        // Node node2{ 2, 0, peers, buffer_size, log_size };
        // node(node0, workers);
        // node(node1, workers);
        // node(node2, workers);
        //
        // std::this_thread::sleep_for(std::chrono::seconds(2));
        // client(Address{"127.0.0.1", 7069}, 2, 100000, 10000, 8000, workers);

        int sig;
        while (RUNNING.load()) {
            if (sigwait(&sigset, &sig) == 0) {
                std::cout << "Received signal: " << sig << std::endl;
                RUNNING.store(false);
            }
        }


        std::cout << "Going to join all the workers" << std::endl;

        for (std::thread &worker: workers) {
            worker.join();
        }

        std::cout << "Shutting down..." << std::endl;
    } catch (std::exception &e) {
        std::cerr << e.what() << std::endl;
    }
}

unsigned int getEnvUInt(const char *name) {
    const char *val = std::getenv(name);
    if (!val) throw std::runtime_error("Environment variable " + std::string(name) + " is not set");

    try {
        return static_cast<unsigned int>(std::stoul(val));
    } catch (...) {
        throw std::invalid_argument(std::string("Invalid uint for env var: ") + name);
    }
}

std::vector<Address> getEnvPeers(const char *env_var_name) {
    const char *val = std::getenv(env_var_name);
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

Address getEnvAddress(const char *env_var_name) {
    const char *val = std::getenv(env_var_name);
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

