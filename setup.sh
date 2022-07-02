dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/processMeerKAT"
export PATH=$PATH:$dir
export PYTHONPATH=$PYTHONPATH:$dir
export SINGULARITYENV_PYTHONPATH="$PYTHONPATH:\$PYTHONPATH"
