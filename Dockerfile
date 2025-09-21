FROM alpine:latest
LABEL authors="spike"

FROM golang:1.21-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s -X main.version=${VERSION} -X main.buildTime=${BUILD_TIME}" -o main .

FROM alpine:latest

RUN apk --no-cache add ca-certificates
RUN apk --no-cache add \
    \
    arp-scan \
    iputils \      \
    net-tools \   \
    tcpdump \      \
    curl \        \
    nmap \        \
    && rm -rf /var/cache/apk/*

RUN addgroup -S appgroup && adduser -S appuser -G appgroup

WORKDIR /root/

COPY --from=builder /app/main .

COPY --from=builder /app/configs configs/
COPY --from=builder /app/.env .env

RUN chown -R appuser:appgroup /app

USER appuser

EXPOSE 8080

CMD ["./main"]
ENTRYPOINT ["top", "-b"]