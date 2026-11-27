FROM python:3.9 AS build

WORKDIR /home/recceiver

COPY . ./server

RUN python -m venv venv

RUN venv/bin/python -m pip install "./server[metrics]"

FROM python:3.9-alpine

RUN adduser -D -h /home/recceiver recceiver

WORKDIR /home/recceiver

COPY --from=build /home/recceiver/venv venv

USER recceiver

CMD ["venv/bin/twistd", "--pidfile=", "--nodaemon", "recceiver"]
