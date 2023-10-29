FROM ubuntu:jammy

ARG DEBIAN_FRONTEND=noninteractive
RUN apt update && apt -y install vim python3 python3-pip unzip cron \
    && apt -y install less libpq-dev postgresql-client

COPY config/cloud-sql-proxy /root/cloud-sql-proxy 
RUN chmod -R a+x /root/cloud-sql-proxy

RUN mkdir /root/.ssh
COPY config/service-client.json /root/.ssh/service-client.json
COPY config/settings.json config/trading-app /root/
RUN chmod -R +rx /root

ARG key
ARG secret
ARG dbpass

ENV API_KEY $key
ENV API_SECRET $secret
ENV DB_PASSWORD $dbpass
ENV GOOGLE_APPLICATION_CREDENTIALS="/root/.ssh/service-client.json"

# CMD [ "sleep", "infinity" ]
CMD ["root/trading-app", "--settings", "/root/settings.json"]
