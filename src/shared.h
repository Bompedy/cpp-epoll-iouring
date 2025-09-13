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
    sockaddr_in addr_{};

public:
    Address(const std::string& host, const unsigned short port) {
        std::memset(&addr_, 0, sizeof(addr_));
        addr_.sin_family = AF_INET;
        addr_.sin_port = htons(port);
        if (inet_pton(AF_INET, host.c_str(), &addr_.sin_addr) <= 0) {
            throw std::invalid_argument("Invalid IPv4 address: " + host);
        }
    }

    [[nodiscard]] std::string host() const {
        char ip_str[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &addr_.sin_addr, ip_str, INET_ADDRSTRLEN);
        return std::string{ip_str};
    }

    [[nodiscard]] unsigned short port() const {
        return ntohs(addr_.sin_port);
    }

    bool operator==(const Address &other) const {
        return addr_.sin_addr.s_addr == other.addr_.sin_addr.s_addr &&
               addr_.sin_port == other.addr_.sin_port;
    }

    [[nodiscard]] sockaddr *sockaddr_ptr() {
        return reinterpret_cast<sockaddr *>(&addr_);
    }

    [[nodiscard]] const sockaddr *sockaddr_ptr() const {
        return reinterpret_cast<const sockaddr *>(&addr_);
    }

    [[nodiscard]] socklen_t sockaddr_len() const {
        return sizeof(addr_);
    }
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

inline int setup_server_socket(const std::string &address, const unsigned short port) {
    constexpr int opt = 1;
    const auto server_fd = socket(AF_INET, SOCK_DGRAM, 0);

    if (server_fd < 0) {
        throw std::runtime_error("server socket failed: " + std::string(std::strerror(errno)));
    }
    //
    // if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt)) < 0) {
    //     close(server_fd);
    //     throw std::runtime_error("Failed to set reuseport to socket");
    // }

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

long time_millis() {
    auto now = std::chrono::system_clock::now();
    auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(
            now.time_since_epoch()
    ).count();
    return millis;
}

// Returns the port number the socket is bound to, or -1 on error
int get_bound_port(int fd) {
    struct sockaddr_in sa;
    socklen_t len = sizeof(sa);
    if (getsockname(fd, (struct sockaddr*)&sa, &len) == -1) {
        perror("getsockname failed");
        return -1;
    }
    return ntohs(sa.sin_port);
}

// Example usage:
void print_bound_port(int fd) {
    int port = get_bound_port(fd);
    if (port != -1) {
        std::cout << "FD " << fd << " bound to port " << port << std::endl;
    } else {
        std::cout << "Failed to get port for FD " << fd << std::endl;
    }
}

inline std::atomic RUNNING{true};