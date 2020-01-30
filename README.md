# mailroom [![Build Status](https://github.com/nyaruka/mailroom/workflows/CI/badge.svg)](https://github.com/nyaruka/mailroom/actions?query=workflow%3ACI) [![codecov](https://codecov.io/gh/nyaruka/mailroom/branch/master/graph/badge.svg)](https://codecov.io/gh/nyaruka/mailroom)

# About 

Mailroom is the [RapidPro](https://github.com/rapidpro/rapidpro) component responsible for the execution of
flows. It interacts directly with the RapidPro database and sends and receives messages with [Courier](https://github.com/nyaruka/courier) for handling via Redis.

# Deploying

As Mailroom is a go application, it compiles to a binary and that binary along with the config file is all
you need to run it on your server. You can find bundles for each platform in the
[releases directory](https://github.com/nyaruka/mailroom/releases). We recommend running Mailroom
behind a reverse proxy such as nginx or Elastic Load Balancer that provides HTTPs encryption.

# Configuration

Mailroom uses a tiered configuration system, each option takes precendence over the ones above it:
 1. The configuration file
 2. Environment variables starting with `MAILROOM_` 
 3. Command line parameters

We recommend running Mailroom with no changes to the configuration and no parameters, using only
environment variables to configure it. You can use `% mailroom --help` to see a list of the
environment variables and parameters and for more details on each option.

# RapidPro Configuration

For use with RapidPro, you will want to configure these settings:

 * `MAILROOM_ADDRESS`: the address to bind our web server to (default "localhost")
 * `MAILROOM_DOMAIN`: the domain that mailroom is listening on
 * `MAILROOM_AUTH_TOKEN`: the token clients will need to authenticate web requests (should match setting in RapidPro)
 * `MAILROOM_ATTACHMENT_DOMAIN`: the domain that will be used for relative attachments in flows
 * `MAILROOM_DB`: URL describing how to connect to the RapidPro database (default "postgres://temba:temba@localhost/temba?sslmode=disable")
 * `MAILROOM_REDIS`: URL describing how to connect to Redis (default "redis://localhost:6379/15")
 * `MAILROOM_ELASTIC`: URL describing how to connect to ElasticSearch (default "http://localhost:9200")
 * `MAILROOM_SMTP_SERVER`: the smtp configuration for sending emails ex: smtp://user%40password@server:port/?from=foo%40gmail.com
 
For writing of message attachments, Mailroom needs access to an S3 bucket, you can configure access to your bucket via:

 * `MAILROOM_S3_REGION`: The region for your S3 bucket (ex: `eu-west-1`)
 * `MAILROOM_S3_MEDIA_BUCKET`: The name of your S3 bucket (ex: `dl-mailroom`)
 * `MAILROOM_S3_MEDIA_PREFIX`: The prefix to use for filenames of attachments added to your bucket (ex: `attachments`)
 * `MAILROOM_AWS_ACCESS_KEY_ID`: The AWS access key id used to authenticate to AWS
 * `MAILROOM_AWS_SECRET_ACCESS_KEY` The AWS secret access key used to authenticate to AWS

Recommended settings for error and performance monitoring:

 * `MAILROOM_LIBRATO_USERNAME`: The username to use for logging of events to Librato
 * `MAILROOM_LIBRATO_TOKEN`: The token to use for logging of events to Librato
 * `MAILROOM_SENTRY_DSN`: The DSN to use when logging errors to Sentry
 * `MAILROOM_LOG_LEVEL`: the logging level mailroom should use (default "error", use "debug" for more)

# Development

Install Mailroom source in your workspace with:

```
go get github.com/nyaruka/mailroom
```

Build Mailroom with:

```
go build github.com/nyaruka/mailroom/cmd/mailroom
```

This will create a new executable in your current directory `mailroom`

To run the tests you need to create the test database:

```
$ createdb mailroom_test
$ createuser -P -E -s mailroom_test (set no password)
```

To run all of the tests:

```
go test github.com/nyaruka/mailroom/... -p=1
```
