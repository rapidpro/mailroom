FROM golang:1.13.3-alpine3.10

WORKDIR /app

RUN apk update \
  && apk add --virtual build-deps gcc git curl tar \
  && rm -rf /var/cache/apk/*

RUN addgroup -S golang \
  && adduser -S -G golang golang

COPY . .

RUN curl https://codeload.github.com/nyaruka/goflow/tar.gz/$(grep goflow go.mod | cut -d" " -f2) | tar --wildcards --strip=1 -zx "*/docs/*"

RUN go build ./cmd/...
RUN chmod +x mailroom
RUN chown -R golang /app

USER golang

EXPOSE 8000
ENTRYPOINT ["./mailroom"]
