FROM rust:bookworm AS builder
WORKDIR /build
COPY Cargo.toml ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 appuser
WORKDIR /app
COPY --from=builder /build/target/release/room-discord-rs /app/room-discord-rs
USER appuser
EXPOSE 7860
CMD ["/app/room-discord-rs"]
