dir=$(dirname $BASH_SOURCE)/processMeerKAT
export PATH=$PATH:$dir
export PYTHONPATH=$PYTHONPATH:$dir
export SINGULARITYENV_PYTHONPATH="$PYTHONPATH:\$PYTHONPATH"
git config --global --add safe.directory $(dirname $BASH_SOURCE)
echo This branch of the pipeline \(sourced from $dir\) will be removed on 30 August 2022. To use the latest version of the pipeline \(v2.0\), please source the master branch.
