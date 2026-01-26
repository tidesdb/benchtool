#!/bin/bash

# RocksDB Installation Script with AddressSanitizer (ASan)
# This script installs RocksDB from source with ASan instrumentation
# Usage: ./install_rocksdb_asan.sh [--with-jemalloc]

set -e  # Exit on error

# Parse command line arguments
USE_JEMALLOC=false
if [ "$1" == "--with-jemalloc" ]; then
    USE_JEMALLOC=true
    echo "WARNING: Using jemalloc with ASan may cause conflicts!"
    echo "Press Ctrl+C to cancel, or wait 5 seconds to continue..."
    sleep 5
fi

echo "========================================="
echo "RocksDB Installation Script (with ASan)"
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

# Add jemalloc if requested (not recommended with ASan)
if [ "$USE_JEMALLOC" = true ]; then
    DEPENDENCIES+=(libjemalloc-dev)
    echo "Including jemalloc in dependencies (NOT RECOMMENDED with ASan)..."
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

# Build RocksDB with ASan enabled
echo "Building RocksDB with AddressSanitizer using $CPU_CORES parallel jobs..."
echo "(this may take a while, but will be faster with parallel compilation)"
mkdir -p build
cd build

# Configure CMake options with ASan flags
CMAKE_OPTIONS=(
    -DCMAKE_BUILD_TYPE=RelWithDebInfo
    -DWITH_GFLAGS=ON
    -DWITH_SNAPPY=ON
    -DWITH_LZ4=ON
    -DWITH_ZLIB=ON
    -DWITH_BZ2=ON
    -DWITH_ZSTD=ON
    -DPORTABLE=ON
    -DCMAKE_CXX_FLAGS="-fsanitize=address -fno-omit-frame-pointer -g -Wno-restrict -Wno-maybe-uninitialized"
    -DCMAKE_C_FLAGS="-fsanitize=address -fno-omit-frame-pointer -g -Wno-maybe-uninitialized"
    -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=address"
    -DCMAKE_SHARED_LINKER_FLAGS="-fsanitize=address"
)

# Add jemalloc option if requested
if [ "$USE_JEMALLOC" = true ]; then
    CMAKE_OPTIONS+=(-DWITH_JEMALLOC=ON)
    echo "Enabling jemalloc support (may conflict with ASan)..."
else
    CMAKE_OPTIONS+=(-DWITH_JEMALLOC=OFF)
fi

# Run CMake with configured options
cmake .. "${CMAKE_OPTIONS[@]}"

# Use all available cores for compilation
make -j${CPU_CORES}

# Install RocksDB
echo "Installing RocksDB with ASan..."
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
echo "Built with AddressSanitizer: YES"
if [ "$USE_JEMALLOC" = true ]; then
    echo "Built with jemalloc support: YES (may conflict with ASan)"
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
echo "  nm /usr/local/lib/librocksdb.so | grep asan"
if [ "$USE_JEMALLOC" = true ]; then
    echo "  ldd /usr/local/lib/librocksdb.so | grep jemalloc"
fi
echo ""
echo "IMPORTANT: When linking against this library, you must also"
echo "compile your application with -fsanitize=address"
echo ""

# Clean up
cd /tmp
echo "Cleaning up temporary files..."
rm -rf rocksdb

echo "Installation finished successfully!"
echo ""
echo "Note: This RocksDB build is instrumented with AddressSanitizer"
echo "and should ONLY be used for debugging, not for benchmarking."
echo "ASan adds significant performance overhead (~2x slowdown)."
