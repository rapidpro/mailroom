FROM golang:1.12

WORKDIR /app
COPY . .
RUN go build ./cmd/...
RUN chmod +x mailroom

EXPOSE 80
ENTRYPOINT ["./mailroom"]
