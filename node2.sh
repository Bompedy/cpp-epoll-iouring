#!/bin/bash

set -e

echo "Setting up environment for NODE 0..."

export IS_CLIENT=0
export NODE_ID=2
export LEADER_ID=0
export BUFFER_SIZE=11000
export LOG_SIZE=150000
export CLIENT_LISTENER="10.10.1.3:7069"
export PEERS="10.10.1.1:6969,10.10.1.2:6969,10.10.1.3:6969"

./build/epolluringtest