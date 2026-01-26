#!/bin/bash

# TidesDB Installation Script
# This script installs TidesDB from source
# Usage: ./install_tidesdb.sh [--with-mimalloc] [--with-sanitizer]

set -e  # Exit on error

# Parse command line arguments
USE_MIMALLOC=false
USE_SANITIZER=false

for arg in "$@"; do
    case $arg in
        --with-mimalloc)
            USE_MIMALLOC=true
            ;;
        --with-sanitizer)
            USE_SANITIZER=true
            ;;
        --help|-h)
            echo "Usage: ./install_tidesdb.sh [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --with-mimalloc    Build with mimalloc memory allocator"
            echo "  --with-sanitizer   Build with AddressSanitizer and UBSan"
            echo "  --help, -h         Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $arg"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

echo "========================================="
echo "TidesDB Installation Script"
if [ "$USE_MIMALLOC" = true ]; then
    echo "(with mimalloc support)"
fi
if [ "$USE_SANITIZER" = true ]; then
    echo "(with AddressSanitizer/UBSan)"
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
    libsnappy-dev
    zlib1g-dev
    liblz4-dev
    libzstd-dev
)

# Add mimalloc if requested
if [ "$USE_MIMALLOC" = true ]; then
    DEPENDENCIES+=(libmimalloc-dev)
    echo "Including mimalloc in dependencies..."
fi

$SUDO apt-get install -y "${DEPENDENCIES[@]}"

# Clone TidesDB repository
echo "Cloning TidesDB repository..."
cd /tmp
if [ -d "tidesdb" ]; then
    echo "Removing existing tidesdb directory..."
    rm -rf tidesdb
fi

git clone https://github.com/tidesdb/tidesdb.git
cd tidesdb

# Get the latest release tag
echo "Fetching latest release..."
LATEST_TAG=$(git describe --tags `git rev-list --tags --max-count=1`)
echo "Latest TidesDB version: $LATEST_TAG"

git checkout $LATEST_TAG

# Build TidesDB
echo "Building TidesDB using $CPU_CORES parallel jobs..."
echo "(this may take a while)"
mkdir -p build
cd build

# Configure CMake options
CMAKE_OPTIONS=(
    -DCMAKE_BUILD_TYPE=Release
    -DTIDESDB_BUILD_TESTS=OFF
)

# Add mimalloc option if requested
if [ "$USE_MIMALLOC" = true ]; then
    CMAKE_OPTIONS+=(-DTIDESDB_WITH_MIMALLOC=ON)
    echo "Enabling mimalloc support..."
fi

# Add sanitizer option if requested
if [ "$USE_SANITIZER" = true ]; then
    CMAKE_OPTIONS+=(-DTIDESDB_WITH_SANITIZER=ON)
    CMAKE_OPTIONS+=(-DCMAKE_BUILD_TYPE=RelWithDebInfo)
    echo "Enabling AddressSanitizer and UBSan..."
fi

# Run CMake with configured options
cmake .. "${CMAKE_OPTIONS[@]}"

# Use all available cores for compilation
make -j${CPU_CORES}

# Install TidesDB
echo "Installing TidesDB..."
$SUDO make install

# Update library cache
echo "Updating library cache..."
$SUDO ldconfig

# Verify installation
echo ""
echo "========================================="
echo "Installation Complete!"
echo "========================================="
echo "TidesDB version: $LATEST_TAG"
if [ "$USE_MIMALLOC" = true ]; then
    echo "Built with mimalloc: YES"
else
    echo "Built with mimalloc: NO"
fi
if [ "$USE_SANITIZER" = true ]; then
    echo "Built with sanitizers: YES (ASan + UBSan)"
else
    echo "Built with sanitizers: NO"
fi
echo "Compiled using: $CPU_CORES parallel jobs"
echo ""
echo "Library location: /usr/local/lib"
echo "Header location: /usr/local/include/tidesdb"
echo ""
echo "To verify the installation, you can check:"
echo "  ldconfig -p | grep tidesdb"
if [ "$USE_SANITIZER" = true ]; then
    echo "  nm /usr/local/lib/libtidesdb.so | grep asan"
fi
if [ "$USE_MIMALLOC" = true ]; then
    echo "  ldd /usr/local/lib/libtidesdb.so | grep mimalloc"
fi
echo ""

# Clean up
cd /tmp
echo "Cleaning up temporary files..."
rm -rf tidesdb

echo "Installation finished successfully!"
