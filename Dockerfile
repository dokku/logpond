FROM golang:1.26-alpine AS builder

RUN apk add --no-cache build-base

WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .

ENV CGO_ENABLED=1
RUN go build -trimpath -ldflags='-s -w' -o /out/logpond ./cmd/logpond

FROM alpine:3.20

RUN apk add --no-cache ca-certificates

COPY --from=builder /out/logpond /usr/local/bin/logpond

EXPOSE 8080
VOLUME ["/data", "/etc/logpond"]

ENTRYPOINT ["/usr/local/bin/logpond"]
