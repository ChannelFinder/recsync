FROM ghcr.io/epics-containers/epics-base-developer:7.0.9ec5 AS recsync-base

WORKDIR /recsync
COPY . /recsync/
RUN ln -s /epics/support/configure/RELEASE configure/RELEASE.local
RUN make

FROM recsync-base AS ioc-runner

WORKDIR /recsync/bin/${EPICS_HOST_ARCH}

CMD ./demo /recsync/iocBoot/iocdemo/st.cmd
