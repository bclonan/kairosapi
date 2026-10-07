FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY server.go ./
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /kairos .

FROM alpine:3.23
RUN apk add --no-cache ca-certificates && addgroup -S kairos && adduser -S -G kairos kairos
RUN mkdir /data && chown kairos:kairos /data && chmod 700 /data
WORKDIR /app
COPY --from=build /kairos /usr/local/bin/kairos
COPY workflows/example.json /app/workflows/example.json
USER kairos
ENV KAIROS_ADDR=0.0.0.0:8075
ENV KAIROS_DB_PATH=/data/kairos.db
VOLUME ["/data"]
EXPOSE 8075
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 CMD wget -q -O /dev/null http://127.0.0.1:8075/readyz || exit 1
ENTRYPOINT ["/usr/local/bin/kairos"]
