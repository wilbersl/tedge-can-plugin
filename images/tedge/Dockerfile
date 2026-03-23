#Build dpkg package
FROM goreleaser/nfpm as build-stage

# Copy required files
WORKDIR /build
COPY tedge_can /build/tedge_can
COPY operations /build/operations
COPY scripts /build/scripts
COPY nfpm.yaml /build/nfpm.yaml
COPY config /build/config
COPY tedge-can-plugin.service /build/tedge-can-plugin.service
# Build Package
RUN nfpm pkg --packager deb --target /tmp/can-plugin_all.deb

FROM ghcr.io/thin-edge/tedge-demo-main-systemd:latest

# Install CAN Plugin
COPY --from=build-stage /tmp/can-plugin_all.deb /tmp/can-plugin_all.deb
RUN apt-get update \
    && apt-get install -y --no-install-recommends iproute2 \
    && apt-get install -y --no-install-recommends \
    /tmp/can-plugin_all.deb \
    && rm /tmp/can-plugin_all.deb

# Add company firewall root certificate
RUN apt install -y ca-certificates
COPY images/tedge/root.crt /usr/local/share/ca-certificates/
RUN update-ca-certificates

# Add confguration plugin config
COPY images/tedge/config/tedge-configuration-plugin.toml /etc/tedge/plugins/tedge-configuration-plugin.toml
COPY images/tedge/config/tedge-log-plugin.toml /etc/tedge/plugins/tedge-log-plugin.toml
