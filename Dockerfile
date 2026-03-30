ARG ARCH=$TARGETARCH
ARG DEBIAN_VERSION=bookworm

####################################################################################################
# base
####################################################################################################
FROM debian:${DEBIAN_VERSION}-slim as base
ARG ARCH
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates libgcc-s1 libstdc++6 tzdata wget gzip && \
    rm -rf /var/lib/apt/lists/*

ENV ARGO_VERSION=v3.7.9

RUN wget -q https://github.com/argoproj/argo-workflows/releases/download/${ARGO_VERSION}/argo-linux-${ARCH}.gz
RUN gunzip -f argo-linux-${ARCH}.gz
RUN chmod +x argo-linux-${ARCH}
RUN mv ./argo-linux-${ARCH} /usr/local/bin/argo
COPY dist/argo-events-linux-${ARCH} /bin/argo-events
RUN chmod +x /bin/argo-events

####################################################################################################
# argo-events
####################################################################################################
FROM scratch as argo-events
ARG ARCH
COPY --from=base /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=base /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=base /lib /lib
COPY --from=base /usr/lib /usr/lib
COPY --from=base /usr/local/bin/argo /usr/local/bin/argo
COPY --from=base /bin/argo-events /bin/argo-events
ENTRYPOINT [ "/bin/argo-events" ]
