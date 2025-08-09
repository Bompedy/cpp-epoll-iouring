#include "temp.h"
#include <stdexcept>
#include <cstring>
#include <cerrno>
#include <string>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>
#include <pthread.h>
#include <netinet/tcp.h>
#include <io_uring.h>
#include <asm/unistd_64.h>

int io_uring_setup(const unsigned entries, io_uring_params *params) {
    const int ring_fd = syscall(__NR_io_uring_setup, entries, params);
    if (ring_fd < 0) {
        throw std::runtime_error("io_uring_setup failed: " + std::string(std::strerror(errno)));
    }
    return ring_fd;
}

int io_uring_enter(
    const int ring_fd,
    const unsigned int to_submit,
    const unsigned int min_complete,
    const unsigned int flags
) {
    const int result = syscall(__NR_io_uring_enter, ring_fd, to_submit, min_complete, flags, nullptr, 0);
    if (result < 0) {
        throw std::runtime_error("io_uring_enter failed: " + std::string(std::strerror(errno)));
    }
    return result;
}

int io_uring_register(
    const unsigned int ring_fd,
    const unsigned int op,
    void *arg,
    const unsigned int nr_args
) {
    const int result = syscall(__NR_io_uring_register, ring_fd, op, arg, nr_args);
    if (result < 0) {
        throw std::runtime_error("io_uring_register failed: " + std::string(std::strerror(errno)));
    }
    return result;
}


bool tune_socket(
    const int fd,
    const int buffer_size,
    const bool quick_ack,
    const bool no_delay
) {
    // Set non-blocking
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

    // Set TCP_NODELAY (disable Nagle's)
    if (no_delay) {
        const int flag = 1;
        if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag)) < 0) {
            perror("setsockopt TCP_NODELAY failed");
            return false;
        }
    }

    // Set TCP_QUICKACK (disable delayed ACKs)
    if (quick_ack) {
        const int flag = 1;
        if (setsockopt(fd, IPPROTO_TCP, TCP_QUICKACK, &flag, sizeof(flag)) < 0) {
            perror("setsockopt TCP_QUICKACK failed");
            return false;
        }
    }

    return true;
}

unsigned long pack_fd_index_opcode(const int fd, const unsigned int index, const unsigned char opcode) {
    return (static_cast<unsigned long>(fd) << 32) |
           ((static_cast<unsigned long>(index) & 0xFFFFFF) << 8) |
           (opcode & 0xFF);
}

void unpack_fd_index_opcode(const unsigned long data, int &fd, unsigned int &index, unsigned char &opcode) {
    fd = static_cast<int>(data >> 32);
    index = static_cast<unsigned int>((data >> 8) & 0xFFFFFF);
    opcode = static_cast<unsigned char>(data & 0xFF);
}

unsigned long pack_fd_and_index(const int fd, const unsigned int index) {
    return (static_cast<unsigned long>(fd) << 32) | index;
}

void unpack_fd_and_index(const unsigned long data, int &fd, unsigned int &index) {
    fd = static_cast<int>(data >> 32);
    index = static_cast<unsigned int>(data & 0xFFFFFFFF);
}

void pin_thread_to_core(const int core_id) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);

    if (const auto thread = pthread_self(); pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset) != 0) {
        perror("pthread_setaffinity_np");
    }
}


int setup_server_socket(const std::string& address, const int port) {
    constexpr int opt = 1;
    const auto server_fd = socket(AF_INET, SOCK_STREAM, 0);

    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    if (!tune_socket(server_fd)) {
        close(server_fd);
        throw std::runtime_error("Failed to create tune socket");
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    inet_pton(AF_INET, address.c_str(), &addr.sin_addr);
    addr.sin_port = htons(port);

    if (bind(server_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
        close(server_fd);
        throw std::runtime_error("Failed to bind to port " + std::to_string(port));
    }
    if (listen(server_fd, SOMAXCONN) < 0) {
        close(server_fd);
        throw std::runtime_error("Failed to listen on port " + std::to_string(port));
    }

    return server_fd;
}
