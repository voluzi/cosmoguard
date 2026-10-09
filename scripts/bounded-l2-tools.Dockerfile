# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM golang:1.27.2 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
ARG TARGETOS
ARG TARGETARCH
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -o /out/l2probe ./internal/cmd/l2probe && \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -o /out/l2testupstream ./internal/cmd/l2testupstream
# The coordinator updates the driver's target file through exec's stdin.
FROM alpine:3.24
WORKDIR /tmp
COPY --from=build /out/ /usr/local/bin/
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/l2testupstream"]
