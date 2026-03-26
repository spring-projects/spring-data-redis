ARG IMAGE=redis:8.4.0
FROM ${IMAGE}

COPY config /config/
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

EXPOSE 6379 6380 6381 26379 26380 26381

CMD ["entrypoint.sh"]