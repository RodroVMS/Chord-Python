FROM python:3.7.1-slim
MAINTAINER Rodrigo Pino
RUN ["pip", "install", "zmq", "pyzmq", "sortedcontainers"]