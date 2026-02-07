#!/bin/bash
# PollyMorph Shadow Mode Runner - Linux/macOS
# Usage: ./run_shadow.sh [core_number]
#
# This script:
# 1. Sets IS_SHADOW_MODE=true environment variable
# 2. Builds release binary with native CPU optimizations
# 3. Pins the process to a specific CPU core for consistent latency testing

set -e

# Configuration
CORE=${1:-1}  # Default to core 1
SKIP_BUILD=${SKIP_BUILD:-false}
DEBUG_BUILD=${DEBUG_BUILD:-false}

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m' # No Color

step() { echo -e "\n${CYAN}[*] $1${NC}"; }
success() { echo -e "${GREEN}[+] $1${NC}"; }
warn() { echo -e "${YELLOW}[!] $1${NC}"; }
err() { echo -e "${RED}[-] $1${NC}"; }

# Banner
echo -e "${MAGENTA}"
cat << 'EOF'

  ____       _ _       __  __                  _     
 |  _ \ ___ | | |_   _|  \/  | ___  _ __ _ __ | |__  
 | |_) / _ \| | | | | | |\/| |/ _ \| '__| '_ \| '_ \ 
 |  __/ (_) | | | |_| | |  | | (_) | |  | |_) | | | |
 |_|   \___/|_|_|\__, |_|  |_|\___/|_|  | .__/|_| |_|
                 |___/                  |_|          
                    SHADOW MODE RUNNER

EOF
echo -e "${NC}"

# Step 1: Set environment variables
step "Setting environment variables..."
export IS_SHADOW_MODE=true
export RUST_LOG="info,polymorph_hft=debug"
export RUST_BACKTRACE=1
success "IS_SHADOW_MODE=true"

# Step 2: Build release binary
if [ "$SKIP_BUILD" != "true" ]; then
    step "Building release binary with native CPU optimizations..."
    
    export RUSTFLAGS="-C target-cpu=native"
    
    if [ "$DEBUG_BUILD" = "true" ]; then
        BUILD_CMD="cargo build"
        BINARY_DIR="debug"
    else
        BUILD_CMD="cargo build --release"
        BINARY_DIR="release"
    fi
    
    echo -e "Running: ${BUILD_CMD}"
    echo -e "RUSTFLAGS: ${RUSTFLAGS}"
    
    if ! $BUILD_CMD; then
        err "Build failed!"
        exit 1
    fi
    success "Build completed successfully"
else
    warn "Skipping build (SKIP_BUILD=true)"
    BINARY_DIR=${DEBUG_BUILD:+debug}
    BINARY_DIR=${BINARY_DIR:-release}
fi

# Step 3: Verify binary exists
BINARY_PATH="./target/${BINARY_DIR}/polymorph-hft"

if [ ! -f "$BINARY_PATH" ]; then
    err "Binary not found at $BINARY_PATH"
    err "Please run without SKIP_BUILD=true"
    exit 1
fi

success "Binary found: $BINARY_PATH"

# Step 4: Detect OS and set up CPU pinning
step "Starting PollyMorph with CPU affinity..."
echo "  Target Core: $CORE"

echo ""
echo "============================================================"
echo -e "${YELLOW}  SHADOW MODE ACTIVE - No real trades will be executed${NC}"
echo "  Metrics endpoint: http://localhost:9091/metrics"
echo "  Grafana dashboard: http://localhost:3000"
echo "============================================================"
echo ""

# Step 5: Run with CPU affinity
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    # Linux: use taskset
    success "Using taskset for CPU pinning (Linux)"
    exec taskset -c "$CORE" "$BINARY_PATH"
    
elif [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS: no direct CPU pinning, but we can set QoS
    warn "macOS does not support direct CPU pinning"
    warn "Running without affinity (consider using a Linux VM for testing)"
    exec "$BINARY_PATH"
    
else
    warn "Unknown OS: $OSTYPE - running without CPU affinity"
    exec "$BINARY_PATH"
fi
