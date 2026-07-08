# syntax=docker/dockerfile:1

FROM --platform=$BUILDPLATFORM golang:1.25-bookworm AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETOS=linux
ARG TARGETARCH=amd64
ENV CGO_ENABLED=0 \
    GOOS=${TARGETOS} \
    GOARCH=${TARGETARCH}

# Emit the two runtime binaries used by the platform deployment.
RUN go build -trimpath -ldflags="-s -w" -o /out/shoal-embed ./cmd/shoal-embed \
 && go build -trimpath -ldflags="-s -w" -o /out/shoal ./cmd/shoal

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /
COPY --from=build /out/shoal-embed /shoal-embed
COPY --from=build /out/shoal /shoal
USER nonroot:nonroot

# No fixed ENTRYPOINT: choose the binary in Kubernetes `command`, e.g.
#   command: ["/shoal-embed"] args: ["serve", "--data=/var/lib/shoal", "--port=9876"]
#   command: ["/shoal"]       args: ["-listen=:9800", ...]
CMD ["/shoal-embed", "version"]
