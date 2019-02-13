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

