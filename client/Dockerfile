# Download Epics
FROM --platform=$BUILDPLATFORM debian:bookworm-slim AS epics-download-extract
SHELL ["/bin/bash", "-c"]
RUN apt-get update && apt-get install -yq wget git
WORKDIR /var/cache
ARG EPICSVERSION=7.0.8.1
RUN wget -q --show-progress https://epics.anl.gov/download/base/base-$EPICSVERSION.tar.gz \
&& mkdir /epics/ \
&& tar -xf base-$EPICSVERSION.tar.gz -C /epics \
&& rm base-$EPICSVERSION.tar.gz

FROM --platform=$BUILDPLATFORM debian:bookworm-slim AS base

FROM base AS base-amd64
ENV EPICS_HOST_ARCH=linux-x86_64

FROM base AS base-386
ENV EPICS_HOST_ARCH=linux-x86

FROM base AS base-arm64
ENV EPICS_HOST_ARCH=linux-arm

FROM base AS base-arm
ENV EPICS_HOST_ARCH=linux-arm

# Now finally choose the right base image:
FROM base-$TARGETARCH AS build-epics
SHELL ["/bin/bash", "-c"]
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
 && apt-get install --no-install-recommends -yq \
  build-essential \
  ca-certificates \
  curl \
  libreadline-dev \
  telnet \
 && apt-get clean && rm -rf /var/lib/apt/lists/* && rm -rf /var/cache/apt

WORKDIR /epics
COPY --from=epics-download-extract /epics /epics
ARG EPICSVERSION=7.0.8.1
RUN mv base-$EPICSVERSION base
RUN cd base && make -j$(nproc)

FROM build-epics AS recsync-base

WORKDIR /recsync
COPY . /recsync/
RUN mv docker/RELEASE.local configure/RELEASE.local
ENV EPICS_ROOT=/epics
ENV EPICS_BASE=${EPICS_ROOT}/base
RUN make
WORKDIR /recsync/iocBoot/iocdemo

FROM recsync-base AS ioc-runner

CMD /recsync/bin/${EPICS_HOST_ARCH}/demo st.cmd
