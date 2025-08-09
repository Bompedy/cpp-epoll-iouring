#include <csignal>
#include <iostream>
#include <vector>
#include "consensus.h"

std::atomic RUNNING { true };

void shutdown(const int signum) {
    std::cout << "Received signal: " << signum << std::endl;
    RUNNING.store(false);
}

int main() {
    struct sigaction action {};
    action.sa_handler = shutdown;
    sigemptyset(&action.sa_mask);
    action.sa_flags = 0;
    sigaction(SIGINT,  &action, nullptr);
    sigaction(SIGTERM, &action, nullptr);
    sigaction(SIGQUIT, &action, nullptr);
    sigaction(SIGHUP,  &action, nullptr);

    const std::vector instance_configs0{
        InstanceConfig{
            .host_config = { .host="127.0.0.1", .port=6969}, // host_config
            .peers = std::vector {
                Address { "127.0.0.1", 6970 }
            }
        }
    };

    const std::vector instance_configs1{
        InstanceConfig{
            .host_config = { .host="127.0.0.1", .port=6970 },
            .peers = std::vector {
                Address{ "127.0.0.1", 6969 }
            }
        }
    };

   Consensus<256> node0(IOType::EPOLL, Algorithm::MULTI_PAXOS, instance_configs0, 1, 1, 4096);
   Consensus<256> node1(IOType::EPOLL, Algorithm::MULTI_PAXOS, instance_configs1, 1, 1, 4096);

    while (RUNNING.load()) {
        pause();
    }

    node0.shutdown();
    node1.shutdown();
    std::cout << "Shutting down..." << std::endl;
}