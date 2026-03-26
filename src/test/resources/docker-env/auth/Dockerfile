ARG IMAGE=redis:8.4.0
FROM ${IMAGE}

COPY config /config/
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

EXPOSE 6382 26382

CMD ["entrypoint.sh"]