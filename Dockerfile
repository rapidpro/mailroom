FROM golang:1.21

# copy our dev certs into the container
# WORKDIR /usr/local/share/ca-certificates
# COPY ./rootCA.pem /usr/local/share/ca-certificates/rootCA.crt
# RUN /usr/sbin/update-ca-certificates

WORKDIR /usr/src/app

# pre-copy/cache go.mod for pre-downloading dependencies and only redownloading them in subsequent builds if they change
COPY go.mod go.sum ./
RUN go mod download && go mod verify

# fetch our docs for our goflow version
RUN grep goflow go.mod | cut -d" " -f2 | cut -c2- > /tmp/goflow_version
RUN curl -L "https://github.com/nyaruka/goflow/releases/download/v`cat /tmp/goflow_version`/docs.tar.gz" | tar zxv

COPY . .
RUN go build -v -o /usr/local/bin/app github.com/nyaruka/mailroom/cmd/mailroom

CMD ["app"]