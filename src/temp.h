#pragma once
#include <string>
#include <linux/io_uring.h>

int io_uring_setup(unsigned entries, io_uring_params *params);
int io_uring_enter(int ring_fd, unsigned to_submit, unsigned min_complete, unsigned flags);
int io_uring_register(unsigned ring_fd, unsigned op, void *arg, unsigned nr_args);

int setup_server_socket(const std::string& address, unsigned short port);
bool tune_socket(int fd, unsigned int buffer_size = 4 * 1024 * 1024, bool quick_ack = true, bool no_delay = true);

unsigned long pack_fd_index_opcode(int fd, unsigned index, unsigned char opcode);
void unpack_fd_index_opcode(unsigned long data, int &fd, unsigned &index, unsigned char &opcode);

unsigned long pack_fd_and_index(int fd, unsigned index);
void unpack_fd_and_index(unsigned long data, int &fd, unsigned &index);

void pin_thread_to_core(int core_id);


