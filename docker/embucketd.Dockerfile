# Avalanche router/embucketd image: the HTTP+S3 gateway the engine drives
# warehouses through (register/GET/POST/DROP, catalog endpoints, auth).
#
# Build the binary on the host first (same linux/x86_64 platform as the
# container), then:
#   cargo build --release -p embucketd \
#     --features state-store,state-store-query,queries
#   docker build -f docker/embucketd.Dockerfile -t embucketd:<tag> .

FROM ubuntu:24.04
RUN apt-get update -o Acquire::AllowInsecureRepositories=true \
    && apt-get install -y --no-install-recommends --allow-unauthenticated \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY target/release/embucketd /usr/local/bin/embucketd
EXPOSE 3000
ENTRYPOINT ["embucketd"]