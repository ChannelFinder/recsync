#!/bin/sh
set -x -e

export EPICS_HOST_ARCH=`sh $HOME/epics-base/startup/EpicsHostArch`

make -j2

[ "$TEST" = "YES" ] || exit 0

lspci

make tapfiles

make -s test-results || find . -name '*.tap' -print0 | xargs -0 -n1 prove -e cat -f
