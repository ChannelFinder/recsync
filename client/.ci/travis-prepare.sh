#!/bin/sh
set -e -x

cat << EOF > configure/RELEASE
EPICS_BASE=$HOME/epics-base
EOF

git clone --depth 10 --branch $BASE https://github.com/epics-base/epics-base.git $HOME/epics-base

export EPICS_HOST_ARCH=`sh $HOME/epics-base/startup/EpicsHostArch`

case "$STATIC" in
static)
    cat << EOF >> "$HOME/epics-base/configure/CONFIG_SITE"
SHARED_LIBRARIES=NO
STATIC_BUILD=YES
EOF
    ;;
*) ;;
esac

case "$CMPLR" in
clang)
  echo "Host compiler is clang"
  cat << EOF >> $HOME/epics-base/configure/os/CONFIG_SITE.Common.$EPICS_HOST_ARCH
GNU         = NO
CMPLR_CLASS = clang
CC          = clang
CCC         = clang++
EOF
  ;;
*) echo "Host compiler is default";;
esac

# requires wine and g++-mingw-w64-i686
if [ "$WINE" = "32" ]
then
  echo "Cross mingw32"
  sed -i -e '/CMPLR_PREFIX/d' $HOME/epics-base/configure/os/CONFIG_SITE.linux-x86.win32-x86-mingw
  cat << EOF >> $HOME/epics-base/configure/os/CONFIG_SITE.linux-x86.win32-x86-mingw
CMPLR_PREFIX=i686-w64-mingw32-
EOF
  cat << EOF >> $HOME/epics-base/configure/CONFIG_SITE
CROSS_COMPILER_TARGET_ARCHS+=win32-x86-mingw
EOF
fi

# set RTEMS to eg. "4.9" or "4.10"
if [ -n "$RTEMS" ]
then
  echo "Cross RTEMS${RTEMS} for pc386"
  curl -L "https://github.com/mdavidsaver/rsb/releases/download/20171203-${RTEMS}/i386-rtems${RTEMS}-trusty-20171203-${RTEMS}.tar.bz2" \
  | tar -C / -xmj

  sed -i -e '/^RTEMS_VERSION/d' -e '/^RTEMS_BASE/d' $HOME/epics-base/configure/os/CONFIG_SITE.Common.RTEMS
  cat << EOF >> $HOME/epics-base/configure/os/CONFIG_SITE.Common.RTEMS
RTEMS_VERSION=$RTEMS
RTEMS_BASE=$HOME/.rtems
EOF
  cat << EOF >> $HOME/epics-base/configure/CONFIG_SITE
CROSS_COMPILER_TARGET_ARCHS += RTEMS-pc386-qemu
EOF

fi

make -C "$HOME/epics-base" -j2
