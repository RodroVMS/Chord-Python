from chord:latest
MAINTAINER Rodrigo Pino
WORKDIR /app
COPY . .
ENTRYPOINT ["python", "run.py"]