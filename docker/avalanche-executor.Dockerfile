# Avalanche warehouse executor image: embucket's Ballista executor wrapper
# (ballista executor + Embucket UDF codecs from avalanche-executor-common).
#
# Build the binary on the host first (same linux/x86_64 platform as the
# container), then:
#   cargo build --release -p avalanche-executor
#   docker build -f docker/avalanche-executor.Dockerfile \
#     -t avalanche-executor:<tag> .

FROM ubuntu:24.04
RUN apt-get update -o Acquire::AllowInsecureRepositories=true \
    && apt-get install -y --no-install-recommends --allow-unauthenticated \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY target/release/avalanche-executor /usr/local/bin/avalanche-executor
ENTRYPOINT ["avalanche-executor"]
