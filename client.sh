
set -e

echo "Setting up environment for CLIENT..."

export IS_CLIENT=1
export LEADER_ADDRESS="10.10.1.1:7069"
export HOST_ADDRESS="10.10.1.4:8000"
export CONNECTIONS=10
export OPS=1000000
export READ_RATIO=0.0
export DATA_SIZE=1000

echo "Launching client..."
./build/epolluringtest