FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/shoal-tserver ./cmd/shoal-tserver \
 && CGO_ENABLED=0 go build -trimpath -o /out/shoal-compactor ./cmd/shoal-compactor

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/shoal-tserver /usr/local/bin/shoal-tserver
COPY --from=build /out/shoal-compactor /usr/local/bin/shoal-compactor
