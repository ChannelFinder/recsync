FROM python:3.8 as build

WORKDIR /home/recceiver

COPY . ./server

RUN python -m venv venv && \
    venv/bin/pip install \
        --no-cache-dir \
        -i https://artifactory.esss.lu.se/artifactory/api/pypi/pypi-virtual/simple \
        -r server/requirements.txt

RUN venv/bin/python -m pip install ./server

FROM python:3.8-slim

WORKDIR /home/recceiver

COPY --from=build /home/recceiver/venv venv

CMD venv/bin/twistd --pidfile= --nodaemon recceiver --config cf.conf

