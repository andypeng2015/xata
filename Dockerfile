## Common Dockerfile for building service images
## This Dockerfile is used for building the service images in the CI/CD pipeline
## `docker build` is called from services/${SERVICE_NAME}/Makefile

#
# Builder stage: used as cache for building the image in the future
#
FROM --platform=$BUILDPLATFORM golang:1.26.3 AS builder

WORKDIR /xata

COPY go.mod ./
COPY go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download -x

#
# Binary stage: used for building the binary
#
FROM builder AS binary
ARG CGO_ENABLED=0
ARG SERVICE_NAME
ARG SERVICE_PATH

WORKDIR /xata
COPY internal internal
COPY gen gen
COPY proto proto
COPY services services
# Copy saas-services if it exists in the build context (optional for OSS builds)
RUN --mount=type=bind,source=.,target=/src,ro \
    if [ -d /src/saas-services ]; then cp -r /src/saas-services ./; fi
COPY openapi openapi

ARG TARGETOS TARGETARCH
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -buildvcs=true -o server ./${SERVICE_PATH:-services/${SERVICE_NAME}}/cmd

# 🔧 Debug tools stage (builds only if needed)
FROM golang:1.26.3 AS delve
RUN go install github.com/go-delve/delve/cmd/dlv@latest

# 🐞 Final debug image (with Delve)
FROM golang:1.26.3 AS debug
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=binary /xata/server /server
COPY --from=delve /go/bin/dlv /dlv
ENTRYPOINT ["/dlv", "exec", "/server", "--headless", "--listen=:2345", "--api-version=2", "--accept-multiclient", "--continue"]

#
# Final image:
# * Copies the binary from the binary stage
# * It's used for running the service binary in AWS
#
FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=binary /xata/server /server
ENTRYPOINT ["/server"]
