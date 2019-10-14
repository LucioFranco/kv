FROM alpine

COPY target/bin/kv-server /usr/local/bin

ENTRYPOINT ["kv-server"]
