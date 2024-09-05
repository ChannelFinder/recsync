FROM python:3.11 AS build

WORKDIR /home/recceiver

COPY . ./server

RUN python -m venv venv

RUN venv/bin/python -m pip install ./server

FROM python:3.11-alpine

WORKDIR /home/recceiver

COPY --from=build /home/recceiver/venv venv

CMD venv/bin/twistd --pidfile= --nodaemon recceiver --config cf.conf
