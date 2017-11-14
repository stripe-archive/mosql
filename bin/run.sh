#!/bin/bash

scriptdir=$(python -c "import os; print(os.path.realpath('$(dirname $0)'))")
rootdir=$scriptdir/../

export RUBYLIB=$rootdir/lib:$RUBYLIB
exec $rootdir/bin/mosql "$@"
