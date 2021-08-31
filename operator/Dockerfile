FROM python:3.8-slim-buster

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

CMD [ "kopf", "run" , "--liveness=http://0.0.0.0:80/healthz" "logstash-operator.py"]