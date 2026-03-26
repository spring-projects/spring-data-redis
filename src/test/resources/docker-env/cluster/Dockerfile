ARG IMAGE=redis:8.4.0
FROM ${IMAGE}

COPY config /config/
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

EXPOSE 7379 7380 7381 7382

CMD ["entrypoint.sh"]