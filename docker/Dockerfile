FROM ubuntu:jammy

ARG DEBIAN_FRONTEND=noninteractive
RUN apt update && apt -y install vim python3 python3-pip unzip cron \
&& apt -y install less libpq-dev postgresql-client

#COPY config/cloud_sql_proxy /root/cloud_sql_proxy 
#RUN chmod -R a+x /root/cloud_sql_proxy

RUN mkdir /root/.ssh
COPY config/service_client.json /root/.ssh/service_client.json
COPY config/settings.json config/trading-app /root/
RUN chmod -R +rx /root

ARG key
ARG secret

ENV API_KEY $key
ENV API_SECRET $secret
ENV GOOGLE_APPLICATION_CREDENTIALS="/root/.ssh/service_client.json"

#CMD [ "sleep", "infinity" ]
CMD ["root/trading-app", "--settings", "/root/settings.json"]
