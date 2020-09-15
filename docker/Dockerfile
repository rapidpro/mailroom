FROM golang:1.14.6-alpine3.12

WORKDIR /app

RUN apk update \
  && apk add --virtual build-deps gcc git curl tar \
  && rm -rf /var/cache/apk/*

RUN addgroup -S golang \
  && adduser -S -G golang golang

COPY . .

RUN curl https://codeload.github.com/nyaruka/goflow/tar.gz/$(grep goflow go.mod | cut -d" " -f2) | tar --wildcards --strip=1 -zx "*/docs/*"
RUN cp docs/en_US/completion.json docs/completion.json && cp docs/en_US/functions.json docs/functions.json

RUN go install -v ./cmd/...

EXPOSE 8000
ENTRYPOINT ["mailroom"]
