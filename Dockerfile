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

WORKDIR /root/

COPY --from=builder /app/main .

COPY --from=builder /app/configs configs/
COPY --from=builder /app/.env .env

EXPOSE 8080

CMD ["./main"]
ENTRYPOINT ["top", "-b"]