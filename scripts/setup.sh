#!/bin/bash

# Colors
red=$(tput setaf 1)
green=$(tput setaf 2)
blue=$(tput setaf 4)
reset=$(tput sgr0)

# ========================================================================
# @brief createMarketstoreContainer
# Create and initialize marketstore Docker container
function createMarketstoreContainer()
{
  echo "Starting Marketstore container"
  local CONFIG_FILE=$NEPTUNE_ROOT/alpaca/conf/mkts.yml
  sudo docker create --name mktsdb --restart always -p 5993:5993 alpacamarkets/marketstore:latest
  sudo docker cp $CONFIG_FILE mktsdb:/etc/mkts.yml
  sudo docker start -i mktsdb
}

# ========================================================================
# @brief compileProtobuf
# Compile .proto file into Python and C++ bindings
function compileProtobuf()
{
   # Ensure proto compiler (protoc) is installed
   if [ -z "$(which protoc)" ]; then
      sudo snap install protobuf --classic
   fi

   # Create folder for compiled messages
   PROTO_OUTPUT=$NEPTUNE_ROOT/neptune/interface/generated
   mkdir -p "$PROTO_OUTPUT"
   touch $PROTO_OUTPUT/__init__.py
   rm -f $PROTO_OUTPUT/*pb* 2> /dev/null || true

   # Location of .proto files
   PROTO_INPUT=$NEPTUNE_ROOT/neptune/interface

   # Compile protoc
   printf "Generating $PROTO_OUTPUT/interface_pb2.py..."
   protoc -I=$PROTO_INPUT --python_out=$PROTO_OUTPUT $PROTO_INPUT/interface.proto
   protoc -I=$PROTO_INPUT --cpp_out=$PROTO_OUTPUT $PROTO_INPUT/interface.proto

   if [ -f $PROTO_OUTPUT/interface_pb2.py ]; then
      printf "${green}SUCCESS\n${reset}"
   else
      printf "${red}FAILED\n${reset}"
   fi
}

# ========================================================================
# @brief updateAlpacaPackages
# Create and run marketstore container
function updateAlpacaPackages()
{
  echo "Updating Alpaca Trade API"
  python -m pip install --upgrade pip
  pip install git+https://github.com/alpacahq/alpaca-trade-api-python
  pip install git+https://github.com/alpacahq/pymarketstore
  pip install git+https://github.com/mariostoev/finviz
  pip install git+https://github.com/twopirllc/pandas-ta.git@development
}

# ========================================================================
# @brief installProtobuf
# Clone and install Google protocol buffers
function installProtobuf()
{
    sudo apt-get install autoconf automake libtool curl make g++ unzip
    pushd /home/$USER/git || exit
    git clone https://github.com/protocolbuffers/protobuf.git
    pushd protobuf || exit
    git submodule update --init --recursive
    ./autogen.sh
    ./configure
    make
    make check
    sudo make install
    sudo ldconfig # refresh shared library cache.
}

function installZMQ()
{
    sudo apt-get install libtool pkg-config build-essential autoconf automake
    sudo apt-get install libzmq-dev
    pushd /tmp
    git clone git://github.com/jedisct1/libsodium.git
    pushd libsodium
    ./autogen.sh
    ./configure && make check
    sudo make install
    sudo ldconfig

    wget https://github.com/zeromq/libzmq/releases/download/v4.3.4/zeromq-4.3.4.tar.gz
    tar -xvf zeromq-4.3.4.tar.gz
    cd zeromq-4.3.4
    ./autogen.sh
    ./configure && make check
    sudo make install
    sudo ldconfig
}

function helpNeptune()
{
    local script=$NEPTUNE_ROOT/scripts/setup.sh

    # shellcheck disable=SC2059
    printf "${blue}========================== Neptune ===========================\n${reset}"
    printf "Welcome to Neptune, ${green}%s\n${reset}" $USER


    printf "${blue}\n------------------------ Environment -------------------------\n${reset}"
    printf "NEPTUNE_ROOT: %s\n" $NEPTUNE_ROOT

    printf "${blue}\n----------------------- Function Help ------------------------\n${reset}"
    sed -n "/@brief/,/function/p" $script | \
        grep -v sed | \
        sed 's/function.*//g' | \
        sed 's|#\s*||g' | \
        sed -z 's|\(.*\)\n$|\1|'

    printf "${blue}==============================================================\n${reset}"
}


# Configure Neptune root
SCRIPTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export NEPTUNE_ROOT="$(dirname "$SCRIPTS_DIR")"

# Configure PYTHONPATH
if [ -z "${PYTHONPATH}" ]; then
  export PYTHONPATH=$NEPTUNE_ROOT
else
  export PYTHONPATH=$NEPTUNE_ROOT:$PYTHONPATH
  PYTHONPATH="$(perl -e 'print join(":", grep { not $seen{$_}++ } split(/:/, $ENV{PYTHONPATH}))')"
fi

helpNeptune
