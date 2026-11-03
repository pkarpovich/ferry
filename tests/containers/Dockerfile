FROM rust:1.99-slim AS build
WORKDIR /src
RUN rustup target add "$(uname -m)-unknown-linux-musl"
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --locked --target "$(uname -m)-unknown-linux-musl" \
    && cp "target/$(uname -m)-unknown-linux-musl/debug/ferry" /ferry

FROM debian:13-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends openssh-server openssh-client autossh python3 curl procps \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --shell /bin/sh ferry \
    && mkdir -p /run/sshd
COPY --from=build /ferry /opt/ferry/bin/ferry
COPY tests/containers/ /opt/test/
RUN chmod 755 /opt/test/entrypoint.sh /opt/test/handlers/* \
    && su ferry -c 'mkdir -p ~/.ssh ~/.config/ferry/handlers ~/docs \
        && chmod 700 ~/.ssh \
        && ssh-keygen -q -t ed25519 -N "" -f ~/.ssh/dialer_key \
        && ssh-keygen -q -t ed25519 -N "" -f ~/.ssh/tunnel_key \
        && cp /opt/test/handlers/* ~/.config/ferry/handlers/'
ENTRYPOINT ["/opt/test/entrypoint.sh"]
