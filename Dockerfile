FROM golang:1.21-alpine AS builder

RUN apk add --no-cache gcc musl-dev sqlite-dev

WORKDIR /app

COPY go.mod go.sum* ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=1 GOOS=linux go build -a -installsuffix cgo -o scheduler .

FROM alpine:latest

RUN apk --no-cache add ca-certificates sqlite-libs

WORKDIR /root/

COPY --from=builder /app/scheduler .

EXPOSE 8080

ENV MODE=standalone
ENV PORT=8080
ENV DB_PATH=/data/scheduler.db

RUN mkdir -p /data

CMD ["sh", "-c", "./scheduler -mode ${MODE} -port ${PORT} -db ${DB_PATH} ${EXTRA_ARGS}"]
