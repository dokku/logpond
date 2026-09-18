FROM golang:1.27-bookworm AS builder

RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential bash curl ca-certificates jq \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

RUN bash scripts/vendor.sh

ARG VERSION=0.0.0-dev
ENV CGO_ENABLED=1
RUN go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /out/logpond ./cmd/logpond

FROM debian:bookworm-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /out/logpond /usr/local/bin/logpond

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/logpond"]
