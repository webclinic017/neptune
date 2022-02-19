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
# @brief updateAlpacaPackages
# Create and run marketstore container
function updateAlpacaPackages()
{
  echo "Updating Alpaca Trade API"
  python -m pip install --upgrade pip
  pip install git+https://github.com/alpacahq/alpaca-trade-api-python
  pip install git+https://github.com/alpacahq/pymarketstore
  pip install git+https://github.com/mariostoev/finviz
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
