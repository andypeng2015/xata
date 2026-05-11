FROM --platform=$BUILDPLATFORM golang:1.26.3 AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download -x

COPY services/branch-operator/api ./services/branch-operator/api
COPY services/scale-to-zero-sidecar ./services/scale-to-zero-sidecar

ARG TARGETOS TARGETARCH
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -o server ./services/scale-to-zero-sidecar/cmd

FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /app/server /server
ENTRYPOINT ["/server"]
