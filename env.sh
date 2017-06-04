#/bin/bash

#script to source the required environment
CWD="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export PATH=$PATH:$CWD"/sbin/"
export PATH=$PATH:$CWD"/bin/"

# export pythonpath for loading modules
export PYTHONPATH=$PYTHONPATH:$CWD"/sbin/"
echo $PYTHONPATH
