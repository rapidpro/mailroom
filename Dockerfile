FROM golang:1.11

WORKDIR /app
COPY . .
RUN go build ./cmd/...
RUN chmod +x mailroom

EXPOSE 80
ENTRYPOINT ["./mailroom"]
