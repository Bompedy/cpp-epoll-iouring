#pragma once

#include <atomic>
#include <cstring>
#include <vector>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <fcntl.h>

constexpr unsigned char OP_CLIENT_REQUEST = 0;
constexpr unsigned char OP_CLIENT_RESPONSE = 1;
constexpr unsigned char OP_PROPOSE = 2;
constexpr unsigned char OP_ACK = 3;
constexpr unsigned char OP_COMMIT = 4;

constexpr unsigned char REQUEST_WRITE = 0;
constexpr unsigned char REQUEST_READ = 1;

class Address {
    sockaddr_storage storage_{};
    sockaddr *sock_;
    std::string host_;
    unsigned short port_;

public:
    Address(std::string host, const unsigned short port) : host_(std::move(host)), port_(port) {
        std::memset(&storage_, 0, sizeof(storage_));

        // Fill in IPv4 sockaddr_in
        auto *addr_in = reinterpret_cast<sockaddr_in *>(&storage_);
        addr_in->sin_family = AF_INET;
        addr_in->sin_port = htons(port_);
        if (inet_pton(AF_INET, host_.c_str(), &addr_in->sin_addr) <= 0) {
            throw std::invalid_argument("Invalid IPv4 address: " + host_);
        }

        sock_ = reinterpret_cast<sockaddr *>(&storage_);
    }

    [[nodiscard]] const std::string &host() const { return host_; }
    [[nodiscard]] unsigned short port() const { return port_; }
    bool operator==(const Address &other) const {
        return host_ == other.host_ && port_ == other.port_;
    }

    [[nodiscard]] sockaddr *sockaddr_ptr() const { return sock_; }
    [[nodiscard]] socklen_t sockaddr_len() const { return sizeof(sockaddr_in); }
};

struct BufferPool {
    std::vector<char *> free_buffers;
    size_t buffer_size;
    std::mutex mutex;

    BufferPool(size_t buffer_count, size_t buffer_size)
            : buffer_size(buffer_size) {
        for (size_t i = 0; i < buffer_count; ++i) {
            free_buffers.push_back(new char[buffer_size]);
        }
    }

    ~BufferPool() {
        for (char *buf: free_buffers) {
            delete[] buf;
        }
    }

    char *acquire() {
        if (free_buffers.empty()) {
            return new char[buffer_size];
        }
        char *buf = free_buffers.back();
        free_buffers.pop_back();
        return buf;
    }

    void release(char *buffer) {
        free_buffers.push_back(buffer);
    }

    size_t available() {
        return free_buffers.size();
    }
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

long time_millis() {
    auto now = std::chrono::system_clock::now();
    auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(
            now.time_since_epoch()
    ).count();
    return millis;
}