#include <csignal>
#include <iostream>
#include <sstream>
#include <bits/std_thread.h>
#include <bits/this_thread_sleep.h>
#include "shared.h"
#include "node.h"
#include "client.h"

unsigned int getEnvUInt(const char *name);
std::vector<Address> getEnvPeers(const char *env_var_name);
Address getEnvAddress(const char *env_var_name);

std::atomic RUNNING{true};

void shutdown(const int signum) {
    std::cout << "Received signal: " << signum << std::endl;
    RUNNING.store(false);
}

int main() {
    try {
        struct sigaction action{};
        action.sa_handler = shutdown;
        sigemptyset(&action.sa_mask);
        action.sa_flags = 0;
        sigaction(SIGINT, &action, nullptr);
        sigaction(SIGTERM, &action, nullptr);
        sigaction(SIGQUIT, &action, nullptr);
        sigaction(SIGHUP, &action, nullptr);

        std::vector<std::thread> workers;

        std::vector<Address> peers = {
            Address{"127.0.0.1", 6969},
            Address{"127.0.0.1", 6970},
            Address{"127.0.0.1", 6971}
        };

        int buffer_size = 200;
        int log_size = 1001000;
        Node node0{ 0, true, peers, buffer_size, log_size };
        Node node1{ 1, false, peers, buffer_size, log_size };
        Node node2{ 2, false, peers, buffer_size, log_size };
        node(node0, workers, RUNNING);
        node(node1, workers, RUNNING);
        node(node2, workers, RUNNING);

        std::this_thread::sleep_for(std::chrono::seconds(5));
        client(Address{"127.0.0.1", 7069}, 1, 10000, 1, workers, RUNNING);

        while (RUNNING.load()) {
            pause();
        }

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

