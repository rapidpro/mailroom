v5.3.22
----------
 * Make default for MaxBodyBytes 1MB
 * Allow search endpoint to return results without a query
 * Add endpoint to parse contactql query
 * Add config option to max bytes of a webhook call response body

v5.3.21
----------
 * Return field dependencies with queries on contact search endpoint
 * Latest goflow, larger webhook bodies, trim expressions

v5.3.20
----------
 * Update to latest goflow v0.64.9
 * Add contact search web endpoint

v5.3.19
----------
 * Update to goflow v0.64.8

v5.3.18
----------
 * Update to goflow v0.64.7

v5.3.17
----------
 * Include evaluation context with simulation requests

v5.3.16
----------
 * Update to goflow v0.64.2

v5.3.15
----------
 * Update to new goreleaser.yml format

v5.3.14
----------
 * Make webhooks timeout configurable
 * Update to goflow v0.64.0
 * Fix elastic query evaluation when contact field doesn't exist

v5.3.13
----------
 * Update to latest goflow v0.63.1

v5.3.12
----------
 * Still do validation even when migrating to less than the current engine version

v5.3.11
----------
 * Update to latest goflow to add support for flow migrations

v5.3.10
----------
 * Update CreateBroadcastMessages to include globals in context

v5.3.9
----------
 * Update to goflow v0.61.0 and provide email service to engine

v5.3.8
----------
 * Update to goflow v0.60.1

v5.3.7
----------
 * Add support for message topics

v5.3.6
----------
 * Load global assets from database

v5.3.5
----------
 * Update to goflow v0.59.0

v5.3.4
----------
 * Update to goflow v0.58.0

v5.3.3
----------
 * Update to goflow v0.56.3

v5.3.2
----------
 * Update to goflow v0.56.2

v5.3.1
----------
 * Update to latest goflow

v5.3.0
----------
 * Use github actions

v5.2.4
----------
 * Add new dump file with bothub instead of bh

v5.2.3
----------
 * collect queue size 15 seconds after the minute

v5.2.2
----------
 * Update to goflow v0.55.0

v5.2.1
----------
 * Fix NPE when IVR channel not found

v5.2.0
----------
 * RapidPro 5.2 release

v2.1.0
----------
 * Update to goflow v0.54.1

v2.0.44
----------
 * fail calls that have missing channels when retrying them

v2.0.43
----------
 * Simulation should use a different engine instance with a fake airtime service
 * Add bothub classifier service factory

v2.0.42
----------
 * Update to goflow v0.54.0

v2.0.41
----------
 * only release on PG10

v2.0.40
----------
 * Implement hook for airtime_transferred event
 * Use DTOne for airtime service if configured

v2.0.39
----------
 * Update to latest Librato library
 * Audit closing HTTP bodies
 * Add smtp-test command

v2.0.38
----------
 * Latest GoFlow
 * Add Classifier / NLU support for LUIS and Wit.ai

v2.0.37
----------
 * Schedules fired in Mailroom

v2.0.36
----------
 * Fire schedules from Mailroom

v2.0.35
----------
 * Log query execution and elapsed time
 * Update to goflow v0.50.4

v2.0.34
----------
 * Update to latest goflow v0.50.2
 * Add support for triggering sessions via query within a flow

v2.0.33
----------
 * Stop writing to legacy engine fields on flows_flowrun
 * Move tasks into their own package
 * Add ElasticSearch URL to README

v2.0.32
----------
 * Add expression/migrate endpoint

v2.0.31
----------
 * Allow interrupting sessions by flow
 * Update to goflow v0.49.0

v2.0.30
----------
 * Update to goflow v0.47.3

v2.0.29
----------
 * Expire runs and sessions in a transaction to guarantee they are always in sync

v2.0.28
----------
 * Remove debug error message

v2.0.27
----------
 * Remove references to trigger_count in unit tests
 * Fix create contact failing to start

v2.0.26
----------
 * Start writing flows_flowrun.status alongside exit_type

v2.0.25
----------
 * Handle FlowRun having nil Flow, use FlowReference instead

v2.0.24
----------
 * Enable interrupt_sessions task

v2.0.23
----------
 * Use ExitSession when stopping a session due to missing flow
 * Add new interrupt sessions task, use more specific exit for missing flows
 * Deal with handles for contacts that no longer have a URN

v2.0.22
----------
 * Populate context with urns and fields when evaluating broadcast templates

v2.0.21
----------
 * Change default port for Elastic to 9200 and use HTTP.
 * Don't try to sniff cluster (doesn't with with cloud elastic)

v2.0.20
----------
 * fix empty starts not being marked as complete
 * allow flow starts to specify query for contacts to start

v2.0.19
----------
 * Update to goflow v0.45.2

v2.0.18
----------
 * Make FlowSession.uuid nullable for now

v2.0.17
----------
 * Update to goflow v0.45.0
 * Write UUID field on flows_flowsession

v2.0.16
----------
 * write/read parent summary on flow starts
 * fix IVR starts not being able to reference parent

v2.0.15
----------
 * Update to goflow v0.42.0
 * Update test db to remove msgs_broadcast.purged

v2.0.14
----------
 * load extra for start and include when starting IVR calls

v2.0.13
----------
 * fix leaking DB connections causing mailroom deadlock under certain loads

v2.0.12
----------
 * Update to goflow v0.41.18

v2.0.11
----------
 * deal with brand new URNs when sending messages

v2.0.10
----------
 * Update to goflow v0.41.16
 * Fix endpoints so we don't hard error for expected requests

v2.0.9
----------
 * Update to goflow v0.41.14

v2.0.8
----------
 * Update to goflow v0.41.13

v2.0.7
----------
 * Update to goflow v0.41.12

v2.0.6
----------
 * Update to goflow v0.41.11

v2.0.5
----------
 * Update to goflow v0.41.10

v2.0.4
----------
 * Update to goflow v0.41.9

v2.0.3
----------
 * Update to goflow v0.41.8

v2.0.2
----------
 * override default max digits of 4 for nexmo gathers

v2.0.1
----------
 * change missing dependencies to a warning instead of an error (that is logged to sentry)

v2.0.0
----------
 * remove references to unused fields

v1.0.7
----------
 * update to latest gocommon, check channel is nil when determining what to send

v1.0.6
----------
 * evaluate templates in broadcasts, including legacy ones
 * mark broadcast as sent when the last batch is sent

v1.0.5
----------
 * SignalWire IVR handling

v1.0.4
----------
 * also treat initiated as in progress

v1.0.3
----------
 * handle signalwire and twiml IVR calls

v1.0.2
----------
 * Make max number of steps configurable

v1.0.1
----------
 * Update to latest goflow v0.41.7

v1.0.0
----------
 * Update to goflow v0.41.6

v0.0.208
----------
 * latest goflow with UI cloning fix

v0.0.207
----------
 * latest goflow with has phone test

v0.0.206
----------
 * Update to goflow v0.41.3

v0.0.205
----------
 * update test for endpoint

v0.0.204
----------
 * latest goflow, fixing migration for relative attachments with no media

v0.0.203
----------
 * Update to goflow v0.41.1
 * The /flow/inspect endpoint should do optional validation

v0.0.202
----------
 * Update to goflow v0.41.0
 * Add /flow/clone and /flow/inspect endpoints

v0.0.201
----------
 * Update to goflow v0.39.3 to handle malformed single message flows from campaign events

v0.0.200
----------
 * adjust test for not stripping slashes

v0.0.199
----------
 * deal with non-slash ending docs urls

v0.0.198
----------
 * add docs webapp

v0.0.197
----------
 * tweak tar arguments

v0.0.196
----------
 * test build with docs

v0.0.195
----------
 * Update to goflow v0.38.3 and remove satori/uuid dependency

v0.0.194
----------
 * Update to goflow v0.38.2

v0.0.193
----------
 * Update to goflow v0.38.0

v0.0.192
----------
 * latest goflow engine

v0.0.191
----------
 * override name and uuid in definition with db settings

v0.0.190
----------
 * goflow v37.2, fixes text_slice unicode issues

v0.0.189
----------
 * Update to goflow v0.37.1

v0.0.188
----------
 * latest goflow, accept text/javascript webhooks

v0.0.187
----------
 * override expire_after_minutes on saved definition with flow setting

v0.0.186
----------
 * Update to goflow v0.36.0
 * Dont apply events on errored sessions

v0.0.185
----------
 * assign topups to broadcast messages

v0.0.184
----------
 * Update to goflow v0.34.1

v0.0.183
----------
 * Update to goflow v0.34.0

v0.0.182
----------
 * Update to goflow v0.33.9

v0.0.181
----------
 * Update to goflow v0.33.8 (expressions refactor)

v0.0.180
----------
 * Update to goflow v0.33.7

v0.0.179
----------
 * Update to goflow v0.33.6

v0.0.178
----------
 * add option to enable / disable retrying pending messages

v0.0.177
----------
 * dont migrate flows that are version 12 or above

v0.0.176
----------
 * fix issue with timeouts and followup splits

v0.0.175
----------
 * latest goflow, log all errors to session

v0.0.174
----------
 * fix dot lookup on nil values

v0.0.173
----------
 * better logging in case of panic

v0.0.172
----------
 * latest goflow, add support for templates

v0.0.171
----------
 * latest gocommon, phonenumbers

v0.0.170
----------
 * switch to using generic map interface for extra on channel events

v0.0.169
----------
 * move to using our own null.StringMap so channel events are decoded properly

v0.0.168
----------
 * print extra when ignoring event

v0.0.167
----------
 * better testing of ivr for nexmo and twitter

v0.0.166
----------
 * don't log to sentry on failed call starts (channellog created anyways), deal with channels disappering before handling

v0.0.165
----------
 * fix for referral triggers with specific referrers matching others

v0.0.164
----------
 * for nexmo, first look at URL param to see if call is ongong

v0.0.163
----------
 * update method for nexmo call creation

v0.0.162
----------
 * Latest goflow

v0.0.161
----------
 * latest goflow

v0.0.160
----------
 * properly start IVR flows on msg triggers

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

