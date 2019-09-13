FROM golang:1.12

WORKDIR /app

COPY . .

RUN go build ./cmd/... && chmod +x mailroom

EXPOSE 80
ENTRYPOINT ["./mailroom"]