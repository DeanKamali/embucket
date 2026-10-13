# Avalanche warehouse scheduler image: embucket's Ballista scheduler wrapper
# (ballista scheduler + Embucket UDF codecs from avalanche-executor-common).
#
# Build the binary on the host first (same linux/x86_64 platform as the
# container), then:
#   cargo build --release -p avalanche-scheduler
#   docker build -f docker/avalanche-scheduler.Dockerfile \
#     -t avalanche-scheduler:<tag> .

FROM ubuntu:24.04
RUN apt-get update -o Acquire::AllowInsecureRepositories=true \
    && apt-get install -y --no-install-recommends --allow-unauthenticated \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY target/release/avalanche-scheduler /usr/local/bin/avalanche-scheduler
EXPOSE 50050
ENTRYPOINT ["avalanche-scheduler"]
