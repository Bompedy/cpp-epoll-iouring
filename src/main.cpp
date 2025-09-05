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


    constexpr unsigned int num_conn_per_peer = 1;
    constexpr unsigned int pipes_per_instance = 1;
    constexpr unsigned int num_instances = 1;
    const std::vector peer_addrs{
        Address{ "127.0.0.1", 6969 },
        // Address{ "127.0.0.1", 6970 },

        Address{ "127.0.0.1", 6971 }
        // Address{ "127.0.0.1", 6972 }
    };

    const auto nodes = peer_addrs.size() / num_instances;
    std::cout << "N=" << nodes << std::endl;


   Consensus<256> node0(IOType::EPOLL, Algorithm::MULTI_PAXOS, num_instances, 0, 0, num_conn_per_peer, pipes_per_instance, peer_addrs, 10000);
   Consensus<256> node1(IOType::EPOLL, Algorithm::MULTI_PAXOS, num_instances, 0, 1, num_conn_per_peer, pipes_per_instance, peer_addrs, 10000);
   while (RUNNING.load()) {
       pause();
   }

   node0.shutdown();
   node1.shutdown();
   std::cout << "Shutting down..." << std::endl;
}