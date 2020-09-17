FROM golang:1.14.6-alpine3.12

WORKDIR /app

RUN apk update \
  && apk add --virtual build-deps gcc git curl tar \
  && rm -rf /var/cache/apk/*

RUN addgroup -S golang \
  && adduser -S -G golang golang

COPY . .

RUN export GOFLOW_VERSION=$(grep goflow go.mod | cut -d" " -f2 | cut -c2-) && curl https://codeload.github.com/nyaruka/goflow/tar.gz/v$GOFLOW_VERSION | tar --wildcards --strip=1 -zx "goflow-${GOFLOW_VERSION}/docs/*"
RUN cp ./docs/en-us/*.* docs/

RUN go install -v ./cmd/...

EXPOSE 8000
ENTRYPOINT ["mailroom"]
