FROM alpine:latest
LABEL authors="spike"

FROM golang:1.25-alpine AS builder

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s -X main.version=${VERSION} -X main.buildTime=${BUILD_TIME}" -o main .

FROM alpine:latest

RUN apk update && apk --no-cache add \
    ca-certificates \
    arp-scan \
    iputils \
    net-tools \
    tcpdump \
    curl \
    nmap \
    && rm -rf /var/cache/apk/*

RUN addgroup -S appgroup && adduser -S appuser -G appgroup

WORKDIR /root/

COPY --from=builder /main .

COPY --from=builder /configs configs/
COPY --from=builder /db.env db.env
COPY --from=builder /esp.env esp.env
COPY --from=builder /client.env client.env

RUN chown -R appuser:appgroup /

USER appuser

EXPOSE 8080

CMD ["./main"]
# ENTRYPOINT ["top", "-b"]
