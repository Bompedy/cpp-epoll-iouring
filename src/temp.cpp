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
#include <linux/io_uring.h>
#include <asm-generic/unistd.h>

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
        // Construct the error message string first
        std::string err_msg = "io_uring_register buffers failed: ";
        err_msg += strerror(-result);
        err_msg += " (";
        err_msg += std::to_string(result);
        err_msg += ")\n";
        throw std::runtime_error(err_msg);
    }
    return result;
}


bool tune_socket(
    const int fd,
    const unsigned int buffer_size
    // const bool quick_ack,
    // const bool no_delay
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
    // if (no_delay) {
    //     const int flag = 1;
    //     if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag)) < 0) {
    //         perror("setsockopt TCP_NODELAY failed");
    //         return false;
    //     }
    // }
    //
    // // Set TCP_QUICKACK (disable delayed ACKs)
    // if (quick_ack) {
    //     const int flag = 1;
    //     if (setsockopt(fd, IPPROTO_TCP, TCP_QUICKACK, &flag, sizeof(flag)) < 0) {
    //         perror("setsockopt TCP_QUICKACK failed");
    //         return false;
    //     }
    // }

    return true;
}


uint64_t pack_fd_index_opcode(const unsigned int fd, const unsigned int index, const unsigned char opcode) {
    return (static_cast<uint64_t>(fd) << 32) |
           ((static_cast<uint64_t>(index) & 0xFFFFFF) << 8) |
           (opcode & 0xFF);
}

void unpack_fd_index_opcode(const unsigned long data, int &fd, unsigned int &index, unsigned char &opcode) {
    fd = static_cast<int>(data >> 32);
    index = static_cast<unsigned int>((data >> 8) & 0xFFFFFF);
    opcode = static_cast<unsigned char>(data & 0xFF);
}

void pin_thread_to_core(const int core_id) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);

    if (const auto thread = pthread_self(); pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset) != 0) {
        perror("pthread_setaffinity_np");
    }
}


int setup_server_socket(const std::string& address, const unsigned short port, const char* multicast_addr) {
    constexpr int opt = 1;
    const auto server_fd = socket(AF_INET, SOCK_DGRAM, 0);

    if (server_fd < 0) {
        throw std::runtime_error("server socket failed: " + std::string(std::strerror(errno)));
    }

    // Disable loopback immediately after socket creation
    constexpr unsigned char loop = 0;  // Disable loopback
    if (setsockopt(server_fd, IPPROTO_IP, IP_MULTICAST_LOOP, &loop, sizeof(loop)) < 0) {
        close(server_fd);
        throw std::runtime_error("Failed to disable multicast loopback");
    }

    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt)) < 0) {
        close(server_fd);
        throw std::runtime_error("Failed to set reuseport to socket");
    }

    if (!tune_socket(server_fd)) {
        close(server_fd);
        throw std::runtime_error("Failed to create tune socket");
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);  // bind to all interfaces
    addr.sin_port = htons(port);

    if (bind(server_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
        close(server_fd);
        throw std::runtime_error("Failed to bind to port " + std::to_string(port));
    }

    // Join multicast group
    ip_mreq mreq{};
    inet_pton(AF_INET, multicast_addr, &mreq.imr_multiaddr);
    inet_pton(AF_INET, "127.0.0.1", &mreq.imr_interface);  // loopback interface

    if (setsockopt(server_fd, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) < 0) {
        close(server_fd);
        throw std::runtime_error("Failed to add membership to socket");
    }

    // Set multicast interface (loopback)
    in_addr local_interface{};
    inet_pton(AF_INET, "127.0.0.1", &local_interface);
    if (setsockopt(server_fd, IPPROTO_IP, IP_MULTICAST_IF, &local_interface, sizeof(local_interface)) < 0) {
        close(server_fd);
        throw std::runtime_error("Failed to set multicast interface");
    }

    return server_fd;
}