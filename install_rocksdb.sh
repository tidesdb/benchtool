#!/bin/bash

# RocksDB Installation Script for Ubuntu (GCC 12 Compatible)
# This script installs the latest version of RocksDB from source
# Usage: ./install_rocksdb.sh [--with-jemalloc]

set -e  # Exit on error

# Parse command line arguments
USE_JEMALLOC=false
if [ "$1" == "--with-jemalloc" ]; then
    USE_JEMALLOC=true
fi

echo "========================================="
echo "RocksDB Installation Script"
if [ "$USE_JEMALLOC" = true ]; then
    echo "(with jemalloc support)"
else
    echo "(without jemalloc)"
fi
echo "========================================="

# Check if running as root
if [ "$EUID" -ne 0 ]; then 
    SUDO="sudo"
else
    SUDO=""
fi

# Detect number of CPU cores
CPU_CORES=$(nproc)
echo "Detected $CPU_CORES CPU cores - will use all for compilation"

# Update package list
echo "Updating package list..."
$SUDO apt-get update

# Install build dependencies
echo "Installing build dependencies..."
DEPENDENCIES=(
    git
    build-essential
    cmake
    libgflags-dev
    libsnappy-dev
    zlib1g-dev
    libbz2-dev
    liblz4-dev
    libzstd-dev
)

# Add jemalloc if requested
if [ "$USE_JEMALLOC" = true ]; then
    DEPENDENCIES+=(libjemalloc-dev)
    echo "Including jemalloc in dependencies..."
fi

$SUDO apt-get install -y "${DEPENDENCIES[@]}"

# Clone RocksDB repository
echo "Cloning RocksDB repository..."
cd /tmp
if [ -d "rocksdb" ]; then
    echo "Removing existing rocksdb directory..."
    rm -rf rocksdb
fi

git clone https://github.com/facebook/rocksdb.git
cd rocksdb

# Get the latest release tag
echo "Fetching latest release..."
LATEST_TAG=$(git describe --tags `git rev-list --tags --max-count=1`)
echo "Latest RocksDB version: $LATEST_TAG"

git checkout $LATEST_TAG

# Build RocksDB with GCC 12 compatibility flags
echo "Building RocksDB using $CPU_CORES parallel jobs..."
echo "(this may take a while, but will be faster with parallel compilation)"
mkdir -p build
cd build

# Configure CMake options
CMAKE_OPTIONS=(
    -DCMAKE_BUILD_TYPE=Release
    -DWITH_GFLAGS=ON
    -DWITH_SNAPPY=ON
    -DWITH_LZ4=ON
    -DWITH_ZLIB=ON
    -DWITH_BZ2=ON
    -DWITH_ZSTD=ON
    -DPORTABLE=ON
    -DCMAKE_CXX_FLAGS="-Wno-restrict"
)

# Add jemalloc option if requested
if [ "$USE_JEMALLOC" = true ]; then
    CMAKE_OPTIONS+=(-DWITH_JEMALLOC=ON)
    echo "Enabling jemalloc support..."
else
    CMAKE_OPTIONS+=(-DWITH_JEMALLOC=OFF)
fi

# Run CMake with configured options
CXXFLAGS="-Wno-restrict" cmake .. "${CMAKE_OPTIONS[@]}"

# Use all available cores for compilation
make -j${CPU_CORES}

# Install RocksDB
echo "Installing RocksDB..."
$SUDO make install

# Update library cache
echo "Updating library cache..."
$SUDO ldconfig

# Verify installation
echo ""
echo "========================================="
echo "Installation Complete!"
echo "========================================="
echo "RocksDB version: $LATEST_TAG"
if [ "$USE_JEMALLOC" = true ]; then
    echo "Built with jemalloc support: YES"
else
    echo "Built with jemalloc support: NO"
fi
echo "Compiled using: $CPU_CORES parallel jobs"
echo ""
echo "Library location: /usr/local/lib"
echo "Header location: /usr/local/include/rocksdb"
echo ""
echo "To verify the installation, you can check:"
echo "  ldconfig -p | grep rocksdb"
if [ "$USE_JEMALLOC" = true ]; then
    echo "  ldd /usr/local/lib/librocksdb.so | grep jemalloc"
fi
echo ""

# Clean up
cd /tmp
echo "Cleaning up temporary files..."
rm -rf rocksdb

echo "Installation finished successfully!"
echo ""
echo "Usage tip: To install with jemalloc support in the future, run:"
echo "  $0 --with-jemalloc"