v0.0.159
----------
 * update to version 31

v0.0.158
----------
 * fix campaigns based on created_on, fix panic when batch starts on archived flow

v0.0.157
----------
 * Latest goflow

v0.0.156
----------
 * only interrupt sessions of the same type

v0.0.155
----------
 * Update to latest goflow

v0.0.154
----------
 * more verbose logging when failing on run insert

v0.0.153
----------
 * remove all unfired campaign event fires when stopping contacts

v0.0.152
----------
 * send messages for IVR flows as well

v0.0.151
----------
 * retry unhandled messages only once an hour

v0.0.150
----------
 * latest goflow, don't try to validate missing flows

v0.0.149
----------
 * add cron to retry unhandled messages
 * validate flows before resume or start

v0.0.148
----------
 * dont log email sending errors to sentry

v0.0.147
----------
 * better sentry logging on task fails
 * deal with inactive and archived flows in the same way
 * latest goflow

v0.0.146
----------
 * Latest goflow

v0.0.145
----------
 * Update to latest goflow

v0.0.144
----------
 * latest goflow, fix date addition

v0.0.143
----------
 * add support to handle welcome message event as noop

v0.0.142
----------
 * don't throw error when channel doesn't have FCM id

v0.0.141
----------
 * latest goflow, pass in attachment domain

v0.0.140
----------
 * latest goflow, deal with missed mos

v0.0.139
----------
 * Proper parameters for FCM syncing

v0.0.138
----------
 * sync android channels when creating messages, refactor clearing timeouts
 * dont create outgoing ivr messages (internally) when resuming a completed call

v0.0.137
----------
 * Update to latest goflow

v0.0.136
----------
 * Make org_id optional on /mr/flow/validate to validate a flow without assets

v0.0.135
----------
 * properly set priority on outgoing messages
 * update to latest goflow
 * add flow validation endpoint

v0.0.134
----------
 * log channellogs even when we don't have a connection

v0.0.133
----------
 * mark fires as skipped when skipping

v0.0.132
----------
 * GC db connections after 30m, keep 8 around

v0.0.131
----------
 * give ourselves 15 minutes per start

v0.0.130
----------
 * retry transaction on failures

v0.0.129
----------
 * trim contact names to 128 chars

v0.0.128
----------
 * up to 36 redis connections

v0.0.127
----------
 * add db in use metric to librato

v0.0.126
----------
 * create start objects for trigger sessions

v0.0.125
----------
 * mention error count in error
 * retry contact events up to three times
 * remove use of is_active on channel connection

v0.0.124
----------
 * use primary_language_id instead of interface language, allow empty runs

v0.0.123
----------
 * latest goflow, deal with datetime + time additions

v0.0.122
----------
 * fix for PROPER
 * better time addition
 * keep punctuation in remove_first_word

v0.0.121
----------
 * proper settings for skip campaigns

v0.0.120
----------
 * latest goflow, fix date arith
 * include queued on on self queued tasksk

v0.0.119
----------
 * log relative wait and wait_ms to librato, not total
 * record task latency to librato

v0.0.118
----------
 * latest goflow nil is zero, text() for results
 * tweaks to default connection / queue size, better stats

v0.0.117
----------
 * add stats package to log queue size, db connections every minute

v0.0.116
----------
 * audit transaction rollbacks, bit less logging

v0.0.115
----------
 * allow resumes on completed sessions (noops if msg isn't trigger)

v0.0.114
----------
 * latest goflow, more date migrations

v0.0.113
----------
 * latest goflow, fix migration for DAYS

v0.0.112
----------
 * dont trigger on catch alls if we are in a flow

v0.0.111
----------
 * process triggers in simulation

v0.0.110
----------
 * latest goflow, more date tweaks

v0.0.109
----------
 * latest goflow
 * add xdate type
 * fix NPE when referencing @child or @parent

v0.0.108
----------
 * allow resume when we have completed connection status (due to race)

v0.0.107
----------
 * process incoming TWIML events even if a call is completed

v0.0.106
----------
 * more travis attempts

v0.0.105
----------
 * try different condition

v0.0.104
----------
 * use env for testing

v0.0.103
----------
 * test release

v0.0.102
----------
 * try only releasing on 9.6 matrix

v0.0.101
----------
 * use lowercase smtp_config org config

v0.0.100
----------
 * latest goflow with tls renegotation

v0.0.99
----------
 * latest goflow, make sure expirations aren't honored for inactive runs in cases of races

v0.0.98
----------
 * latest goflow engine, change invalid urns to warn
 * add test for IVR timeouts

v0.0.97
----------
 * allow missed calls to start ivr calls
 * refactor queue constants

v0.0.96
----------
 * set timeout when no message has been sent in a session

v0.0.95
----------
 * latest goflow

v0.0.94
----------
 * fix for duplicate fields deletion in single flow only applying one

v0.0.93
----------
 * dont queue messages with no topup assigned, latest goflow
 * try to derive content type for webhook payloads

v0.0.92
----------
 * reset db between tests

v0.0.91
----------
 * mark event fires as fired when starting ivr flows

v0.0.90
----------
 * allow ivr flows to be started via campaigns

v0.0.89
----------
 * fix contact stopping, fix error on status for missed calls

v0.0.88
----------
 * better logging of event in case of error during handling

v0.0.87
----------
 * fix ContactID mismatch

v0.0.86
----------
 * grab contact locks when starting
 * move to nyaruka/null null ints and strings

v0.0.85
----------
 * allow channels to be set in simulations

v0.0.84
----------
 * write webhook events even in simulation case
 * add max value to env
 * add models and tests for webhook events, update libs

v0.0.83
----------
 * associate webhook results with resthook ids

v0.0.82
----------
 * add extra to starts, pass into trigger, add test

v0.0.81
----------
 * log requests when throwing ivr errors
 * don't add attachment domain to geo msg attachments

v0.0.80
----------
 * pass pointer for run insertion

v0.0.79
----------
 * use xml comments in twilio responses for info messages instead of xml element

v0.0.78
----------
 * update tests

v0.0.77
----------
 * add session type to session
 * write both incoming and outgoing surveyor messages
 * newest goflow
 * encapsulate flowrun and flowsession
 * deal with missing fields and groups in surveyor submissions

v0.0.76
----------
 * write webhook results for webhook/resthook actions

v0.0.75
----------
 * use nyaruka sentry so we get breadcrumbs
 * add http_request to errors in web tier so we get more context
 * allow submission of surveyor flows
 * pg 10 support

v0.0.74
----------
 * add nexmo and twilio ivr support

v0.0.73
----------
 * fix location selecting using mptt model

v0.0.72
----------
 * update to latest goflow

v0.0.71
----------
 * optimize getting flow definition

v0.0.70
----------
 * add timings for flow loading

v0.0.69
----------
 * add timings for loading org assets
 * add unit tests for expirations and timeouts

v0.0.68
----------
 * make sure we filter by org id when looking up flows from asserts
 * update msg payload to be in sync with db and courier
 * add timeout and wait_started_on fields to msg sent to courier and session

v0.0.67
----------
 * move to latest goflow
 * update test sql to latest from rapidpro

v0.0.66
----------
 * flush cache before each set of tests
 * more tests, tweaks to timed events

v0.0.65
----------
 * add contact locking when processing contact events

v0.0.64
----------
 * add tests for broadcast batching
 * dont log requests to index page (usually from health checker)

v0.0.63
----------
 * latest goflow, fixes @parent.contact.urn

v0.0.62
----------
 * dont require authorization for / and /mr for status checks
 * mailroom tweaks in support of simulation

v0.0.61
----------
 * fix for single URN broadcasts not sending

v0.0.60
----------
 * v1 of broadcast support
 * lastest goflow

v0.0.59
----------
 * fix datediff units, add session trigger support

v0.0.58
----------
 * add session trigger action

v0.0.57
----------
 * latest goflow, fixes migration of webhook action

v0.0.56
----------
 * update calls to NewMsgIn

v0.0.55
----------
 * latest goflow, fixes case issues

v0.0.54
----------
 * goflow fix for looping

v0.0.53
----------
 * add support for input_labels_added event

v0.0.52
----------
 * latest goflow

v0.0.51
----------
 * add migrate endpoint
 * better status codes in webserver
 * graceful exits

v0.0.50
----------
 * more optimizations for campaign calculations, fix null value case

v0.0.49
----------
 * derive default country from channel countries
 * v1 of simulation endpoint

v0.0.48
----------
 * add support for contact urn changed event, unify updating appropriately

v0.0.47
----------
 * set exited_on and ended_on based on server clock

v0.0.46
----------
 * only wait for sigquit

v0.0.45
----------
 * refactor starts so they pass through same codepath in all cases

v0.0.44
----------
 * correct handling of catch all triggers
 * add support for email created event
 * ignore msg created events that have no urns

v0.0.43
----------
 * mark events as fired even if we end up not creating any sessions

v0.0.42
----------
 * add support for campaign start modes

v0.0.41
----------
 * less logging, only look at expired runs with sessions

v0.0.40
----------
 * latest goflow

v0.0.39
----------
 * change to pkg/errors instead of juju

v0.0.38
----------
 * full handling of timeouts, expirations, handling for flow_server_enabled orgs (alpha)

v0.0.37
----------
 * calculate real tps cost when building messages

v0.0.36
----------
 * update tests for quick replies

v0.0.35
----------
 * proper encoding of quick replies

v0.0.34
----------
 * deal with case of attachment URL not having leading /

v0.0.33
----------
 * resolve relative URLs to absolute URLs

v0.0.32
----------
 * migrate date tests properly in routers

v0.0.31
----------
 * increase our rate of flushing to librato

v0.0.30
----------
 * ignore flow triggered events

v0.0.29
----------
 * remove use of models.ContactID, updated modified_on for contacts that change groups

v0.0.28
----------
 * set status of start to starting after queuing subtasks, update contact count

v0.0.27
----------
 * allow flows to be started through mailroom

v0.0.26
----------
 * fix tests

v0.0.25
----------
 * optimize contact loading queries

v0.0.24
----------
 * ignore (but log) invalid contact urns

v0.0.23
----------
 * more better logging

v0.0.22
----------
 * fix for empty query case, better logging, json env

v0.0.21
----------
 * correct queue names

v0.0.20
----------
 * proper config options

v0.0.19
----------
 * add session commit hook, configurable number of threads

v0.0.18
----------
 * enable mailroom campaigns on all flow_server_enabled flows

v0.0.17
----------
 * add support for group, field, name and language changes
 * add support for recalculating campaign events

v0.0.16
----------
 * correct redaction policy for urns

v0.0.15
----------
 * better logging

v0.0.14
----------
 * more logging of event fires

v0.0.13
----------
 * better elapsed units

v0.0.12
----------
 * tweak librato event names

v0.0.11
----------
 * add librato library
 * add librato logging, catch panics in cron and workers
 * better testing of end state of runner

v0.0.10
----------
 * use bulk inserts for all sessions / runs / messages
 * use same redis caching of topups as rapidpro
 * more tests, updated temba.dump, add testsuite package

v0.0.9
----------
 * deal with no locations for org

v0.0.8
----------
 * clear queuing if there is an error starting task

v0.0.7
----------
 * fire crons one second after the minute, fix multiple contact fires

v0.0.6
----------
 * update goreleaser

v0.0.5
----------
 * full support for simple flows

v0.0.4
----------
 * remove contact fields for now

v0.0.3
----------
 * tweak readme

v0.0.2
----------
 * fix binding
 * queue all messages for contact at once
 * fix broken uuid import

