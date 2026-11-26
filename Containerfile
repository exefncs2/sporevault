# SporeVault — Multi-stage Fedora 45 build (zero external Rust crates)
FROM registry.fedoraproject.org/fedora:45 AS builder
RUN dnf install -y --setopt=install_weak_deps=False rust cargo gcc glibc-devel && dnf clean all
WORKDIR /build
COPY Cargo.toml ./
COPY src ./src
RUN cargo build --release

FROM registry.fedoraproject.org/fedora-minimal:45
LABEL org.opencontainers.image.title="SporeVault"
LABEL org.opencontainers.image.description="Minimal zero-dependency Rust mycelium memory store paired with SporeFlow :9090"
LABEL org.opencontainers.image.source="https://github.com/exefncs2/sporevault"
LABEL org.opencontainers.image.licenses="MIT"
COPY --from=builder /build/target/release/sporevault /usr/local/bin/sporevault
EXPOSE 8081
ENTRYPOINT ["/usr/local/bin/sporevault"]
