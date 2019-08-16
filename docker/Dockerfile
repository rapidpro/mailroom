FROM golang:1.12.9-alpine3.9

WORKDIR /app

RUN apk update \
  && apk add --virtual build-deps gcc git \
  && rm -rf /var/cache/apk/*

RUN addgroup -S golang \
  && adduser -S -G golang golang

COPY . .

RUN go build ./cmd/...
RUN chmod +x mailroom
RUN chown -R golang /app

USER golang

EXPOSE 8000
ENTRYPOINT ["./mailroom"]
