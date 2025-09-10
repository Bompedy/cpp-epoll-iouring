#pragma once
#include <array>
#include <atomic>
#include <memory>
#include <thread>
#include <utility>
#include <vector>
#include "temp.h"


enum class Algorithm : uint8_t {
    MULTI_PAXOS,
    PAXOS,
    RABIA
};
enum class IOType : uint8_t {
    IO_URING,
    EPOLL
};

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

template<size_t log_size>
class Consensus {
    std::array<std::unique_ptr<char[]>, log_size> log;
    std::atomic_char acks[log_size];
    std::atomic<int> committed{};
    std::atomic<int> consumed{};
    std::atomic<bool> running{true};
    std::vector<std::thread> threads;

    void epoll_provider(
        Algorithm algo,
        unsigned int instances,
        unsigned char leader_id,
        unsigned char node_id,
        unsigned int pipes,
        const std::vector<Address> &peers,
        size_t buffer_size
    );

    void io_uring_provider(
        Algorithm algo,
        unsigned int instances,
        unsigned char leader_id,
        unsigned char node_id,
        unsigned int pipes,
        const std::vector<Address> &peers,
        size_t buffer_size
    );
public:
    Consensus(
        IOType io_type,
        Algorithm algo,
        unsigned int instances,
        unsigned char leader_id,
        unsigned char node_id,
        unsigned int pipes,
        const std::vector<Address> &peers,
        size_t buffer_size
    );

    void shutdown();
    ~Consensus();
};

#include "consensus.tpp"