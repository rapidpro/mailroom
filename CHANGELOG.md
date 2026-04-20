v9.0.1 (2024-01-05)
-------------------------
 * Fix interrupting with background flows

v9.0.0 (2024-01-05)
-------------------------
 * Update test database

v8.3.55 (2024-01-02)
-------------------------
 * Fix last seen on for channel events
 * Remove ticketers

v8.3.54 (2023-12-12)
-------------------------
 * Update deps

v8.3.53 (2023-12-08)
-------------------------
 * Fix recording cron times

v8.3.52 (2023-12-06)
-------------------------
 * Don't include templating variables if empty

v8.3.51 (2023-12-05)
-------------------------
 * Save msg templating metdata with original template language

v8.3.50 (2023-12-05)
-------------------------
 * Always use created_on rather than occured_on when updating last_seen on based on a channel event

v8.3.49 (2023-12-04)
-------------------------
 * Change ChannelEvent.created_on to be db time
 * Refactor cron jobs to use an interface

v8.3.48 (2023-11-28)
-------------------------
 * Use TemplateTranslation.locale instead of language and country
 * Simplify loading of templates
 * Update to latest goflow/gocommon/phonenumbers

v8.3.47 (2023-11-20)
-------------------------
 * Update deps
 * Write cron results to redis

v8.3.46 (2023-11-13)
-------------------------
 * Update to latest goflow
 * Remove no longer used endpoint alias

v8.3.45 (2023-11-13)
-------------------------
 * Rework cron functions to return results and do consistent logging
 * Add URNs to broadcast endpoint

v8.3.44 (2023-11-10)
-------------------------
 * Rename preview_start to start_preview for consistency

v8.3.43 (2023-11-10)
-------------------------
 * Rename preview_broadcast to broadcast_preview
 * Add web endpoint to create and send a broadcast

v8.3.42 (2023-11-10)
-------------------------
 * Tweak logging in schedules cron
 * Support is_paused on schedules

v8.3.41 (2023-11-08)
-------------------------
 * Stop reading from Schedule.is_active which is no longer used

v8.3.40 (2023-11-07)
-------------------------
 * Add medium field to notifications and start writing
 * Update test database

v8.3.39 (2023-11-07)
-------------------------
 * Actually delete schedule objects instead of soft deletes

v8.3.38 (2023-11-06)
-------------------------
 * Deactivate schedules along with their broadcast/trigger if they have no more firing to do

v8.3.37 (2023-11-03)
-------------------------
 * Rework schedule firing so that we load the trigger

v8.3.36 (2023-11-02)
-------------------------
 * Rework flow start tasks to not require flow_type

v8.3.35 (2023-11-01)
-------------------------
 * Remove logrus usage and hook sentry to slog
 * Update to latest gocommon

v8.3.34 (2023-10-27)
-------------------------
 * Update to latest goflow
 * Replace more logrus use by slog

v8.3.33 (2023-10-13)
-------------------------
 * Read from Trigger.keywords instead of keyword
 * Update deps
 * Replace more logrus use with slog

v8.3.32 (2023-10-10)
-------------------------
 * Update to latest goflow
 * Fix filtering by trigger optional channel
 * Use more slog, replacing logrus

v8.3.31 (2023-10-05)
-------------------------
 * Update test database

v8.3.30 (2023-10-04)
-------------------------
 * Add optin to channelevent

v8.3.29 (2023-10-02)
-------------------------
 * Fix loading of scheduled broadcast with optin

v8.3.28 (2023-10-02)
-------------------------
 * Ensure child broadcasts are saved with parent's optin

v8.3.27 (2023-10-02)
-------------------------
 * Update to latest goflow
 * Fix scheduled broadcasts and optins

v8.3.26 (2023-09-27)
-------------------------
 * Simplify where we load a single contact and ignore events rather than error where contact no longer exists

v8.3.25 (2023-09-27)
-------------------------
 * Update to latest goflow
 * Simplify queuing of optin requests to courier

v8.3.24 (2023-09-26)
-------------------------
 * Update to latest goflow

v8.3.23 (2023-09-25)
-------------------------
 * Fix loading of channel features and queuing of optin messages
 * Use optin msg type
 * Update to latest goflow
 * Simplify loading org assets of different types
 * Replace more logrus with slog
 * Use optin on broadcast to set urn_auth when queueing to courier

v8.3.22 (2023-09-22)
-------------------------
 * Switch to go 1.21 and start switching to slog

v8.3.21 (2023-09-21)
-------------------------
 * Switch to using optin ids instead of UUIDs
 * Update to latest goflow
 * Handle optin_requested events

v8.3.20 (2023-09-19)
-------------------------
 * Add NOOP handler for optin_created events and queue messages to courier with type
 * Cleanup functions use to load org assets
 * Tweak waiting for locks for contacts
 * Create optin flow triggers for optin channel events
 * Implement loading optin assets

v8.3.19 (2023-09-14)
-------------------------
 * Fix stop contact task name

v8.3.18 (2023-09-14)
-------------------------
 * Add support for optin/optout triggers and channel events

v8.3.17 (2023-09-12)
-------------------------
 * Fix not supporting channel events with extra with non-string values
 * Update test database based on https://github.com/nyaruka/rapidpro/pull/4819

v8.3.16 (2023-09-12)
-------------------------
 * Stop reading ContactURN.auth and remove from model

v8.3.15 (2023-09-11)
-------------------------
 * Start reading and writing ContactURN.auth_tokens

v8.3.14 (2023-09-11)
-------------------------
 * Remove support for delegate channels

v8.3.13 (2023-09-11)
-------------------------
 * Just noop if trying to sync an Android channel that doesn't have an FM ID

v8.3.12 (2023-09-11)
-------------------------
 * Remove encoding URN priority in URN strings as it's not used
 * Remove having auth as a URN param
 * Rework message sending so that URNs are loaded before queueing
 * Update to latest null library and use Map[string] for channel events extra

v8.3.11 (2023-09-05)
-------------------------
 * Update to latest goflow

v8.3.10 (2023-09-04)
-------------------------
 * Update Twilio's list of supported language codes for IVR and do a better job of picking one
 * Update to latest goflow
 * More simplification of db/tx params

v8.3.9 (2023-08-30)
-------------------------
 * Make it easier to create messages in tests
 * Add contact task to delete a message

v8.3.8 (2023-08-29)
-------------------------
 * Use less sqlx and tidy up use of interfaces where functions can take a DB or Tx

v8.3.7 (2023-08-23)
-------------------------
 * Use input_collation when matching msg triggers

v8.3.6 (2023-08-23)
-------------------------
 * Use input_collation set on org

v8.3.5 (2023-08-21)
-------------------------
 * Use low priority tasks for batches of big blasts

v8.3.4 (2023-08-21)
-------------------------
 * Update to latest goflow that replaces input_cleaners with input_collation
 * Test on Postgres 15

v8.3.3 (2023-08-17)
-------------------------
 * Read input cleaners from org
 * Update to latest goflow which changes some stuff about environments

v8.3.2 (2023-08-11)
-------------------------
 * Update to latest goflow

v8.3.1 (2023-08-10)
-------------------------
 * Always use main db to load contacts for msg events from new contacts

v8.3.0 (2023-08-09)
-------------------------
 * Update to latest goflow
 * Update to go 1.20

v8.2.0 (2023-07-31)
-------------------------
 * Add dockerfile for dev

v8.1.66 (2023-07-20)
-------------------------
 * Update deps including gocommon which changes requirement for storage paths to start with slash

v8.1.65 (2023-07-18)
-------------------------
 * Limit how old surveyor submissions can be

v8.1.64 (2023-07-10)
-------------------------
 * Update goflow

v8.1.63 (2023-07-03)
-------------------------
 * Support requesting recordings for Twilio with basic auth

v8.1.62 (2023-06-29)
-------------------------
 * Fix session storage path generation

v8.1.61 (2023-06-28)
-------------------------
 * Write channel logs with channels/ key prefex

v8.1.60 (2023-06-28)
-------------------------
 * Tweak channel log creation to prevent nil slices

v8.1.59 (2023-06-28)
-------------------------
 * Update README

v8.1.58 (2023-06-28)
-------------------------
 * Rename sessions bucket config setting for clarity and remove unused sessions prefix setting
 * Write attached call logs only to S3

v8.1.57 (2023-06-20)
-------------------------
 * Fix redaction of twiml IVR channel logs

v8.1.56 (2023-06-08)
-------------------------
 * Support importing of contacts with non-active statuses
 * Use the user that created an import when applying its modifiers

v8.1.55 (2023-06-05)
-------------------------
 * Stop writing ChannelLog.call
 * Stop returning sample contacts on preview endpoints which now only need to return total count

v8.1.54 (2023-05-25)
-------------------------
 * Add endpoint to generate broadcast preview

v8.1.53 (2023-05-25)
-------------------------
 * Rework firing campaign events so that skipping happens outside of runner

v8.1.52 (2023-05-24)
-------------------------
 * Update to latest goflow

v8.1.51 (2023-05-24)
-------------------------
 * Remove applying started-previously exclusion in runner now that it's applied at batch creation stage
 * Refresh elastic indexes after changes in tests instead of waiting for a second
 * Optimize case when recipients is only specific contacts and no exclusions
 * Rework ResolveRecipients to use elastic

v8.1.50 (2023-05-23)
-------------------------
 * Remove support for passing URNs to flow/preview_start as that's not a thing we do
 * Make the name of the ES index for contacts configurable

v8.1.49 (2023-05-18)
-------------------------
 * Remove support for ticket assignment with a note
 * Add contact/bulk_create endpoint

v8.1.48 (2023-05-15)
-------------------------
 * Fix loading of scheduled triggers
 * Update test database

v8.1.47 (2023-05-11)
-------------------------
 * Still queue a courier message even if fetching the flow fails
 * Stop writing old FlowStart fields

v8.1.46 (2023-05-10)
-------------------------
 * Update to latest null library
 * Read from new flow start fields

v8.1.45 (2023-05-09)
-------------------------
 * Always write new FlowStart fields
 * Flow start batches should read from exclusions and remove legacy fields

v8.1.44 (2023-05-08)
-------------------------
 * Start writing exclusions blob on start batch tasks

v8.1.43 (2023-05-08)
-------------------------
 * Add contact locking to ticket/reopen endpoint

v8.1.42 (2023-05-03)
-------------------------
 * Update to latest goflow which fixes parsing locations with non-ASCII chars

v8.1.41 (2023-05-01)
-------------------------
 * Add contact locking to modify endpoint

v8.1.40 (2023-05-01)
-------------------------
 * Add context paramter to LockContacts so it can error if context is done

v8.1.39 (2023-04-27)
-------------------------
 * Refactor how we lock and unlock contacts

v8.1.38 (2023-04-27)
-------------------------
 * Handled incoming messages should be associated with any open ticket
 * Only load the last opened open ticket for a contact

v8.1.37 (2023-04-20)
-------------------------
 * Add contact/inspect endpoint to return all URNs with channel if there is one

v8.1.36 (2023-04-19)
-------------------------
 * Fix not queuing chat messages as high priority and add contact_last_seen_on
 * Use services for github actions

v8.1.35 (2023-04-18)
-------------------------
 * Fix goreleaser changelog generation and use latest action

v8.1.34 (2023-04-17)
-------------------------
 * Add ticket_id to msg and use to set origin on messages queued to courier
 * Remove fields from courier payload that it doesn't use

v8.1.33 (2023-04-13)
-------------------------
 * Use envelope struct for marshalling courier messages and remove unused fields

v8.1.32 (2023-04-03)
-------------------------
 * Fix not logging bodies of incoming IVR requests

v8.1.31 (2023-03-16)
-------------------------
 * Remove no longer used exit type constants
 * Remove support for broadcasts with an associated ticket

v8.1.30 (2023-03-14)
-------------------------
 * Bump courier http client timeout
 * Use Org.config and Channel.config as JSONB columns
 * Fix YYYY-MM-DD date formats

v8.1.29 (2023-03-13)
-------------------------
 * Don't set msg_type when handling messages as courier is already setting it

v8.1.28 (2023-03-08)
-------------------------
 * Remove msg_type values INBOX and FLOW
 * Re-organize web endpoints so each endpoint is in its own file

v8.1.27 (2023-03-06)
-------------------------
 * Add Msg.created_by and populate for chat and broadcast messages

v8.1.26 (2023-02-27)
-------------------------
 * Update goflow
 * Improve detection of repeated outgoing messages

v8.1.25 (2023-02-22)
-------------------------
 * Support Msg.status = I for outgoing messages that should be retried

v8.1.24 (2023-02-22)
-------------------------
 * Update to latest goflow

v8.1.23 (2023-02-20)
-------------------------
 * Use msg_type = T|V for outgoing messages

v8.1.22 (2023-02-16)
-------------------------
 * Use generics to remove repeated code in server endpoints

v8.1.21 (2023-02-15)
-------------------------
 * Cleanup server and http wrappers

v8.1.20 (2023-02-15)
-------------------------
 * Add endpoint to send a single message
 * Cleanup broadcasts and starts
 * Update test database

v8.1.19 (2023-02-13)
-------------------------
 * Stop writing Broadcast.send_all

v8.1.18 (2023-02-13)
-------------------------
 * Update to latest goflow
 * Support contact query based broadcasts by consolidating broadcast and flow start task code
 * Remove support for sending broadcasts to specific URNs

v8.1.17 (2023-02-09)
-------------------------
 * Update how we create messages from broadcasts and resolve translations

v8.1.16 (2023-02-07)
-------------------------
 * Update to latest goflow

v8.1.15 (2023-02-07)
-------------------------
 * Refactor so that web doesn't import testsuite
 * Test queuing and popping of start flow tasks
 * Convert FlowStart to basic struct for simpler marshalling etc

v8.1.14 (2023-02-06)
-------------------------
 * Simplify FlowStartBatch

v8.1.13 (2023-02-06)
-------------------------
 * Fix unmarshalling start tasks

v8.1.12 (2023-02-06)
-------------------------
 * Refactor tasks

v8.1.11 (2023-02-02)
-------------------------
 * Stop writing quick replies to metadata and fix not writing them to the db

v8.1.10 (2023-02-02)
-------------------------
 * Fix test

v8.1.9 (2023-02-02)
-------------------------
 * Update to latest goflow which updates ANTLR
 * Ensure quick replies are included with retries and resends

v8.1.8 (2023-02-02)
-------------------------
 * Start writing Msg.quick_replies as well as writing them to Msg.metadata

v8.1.7 (2023-02-01)
-------------------------
 * Don't send machine_detection param to Nexmo if empty

v8.1.6 (2023-02-01)
-------------------------
 * Update to nyaruka/null v2 and validator v10

v8.1.5 (2023-01-31)
-------------------------
 * Rework more task types to use tasks package
 * Stop adding language and country to msg.metadata.templating

v8.1.4 (2023-01-26)
-------------------------
 * Start writing msgs_msg.locale

v8.1.3 (2023-01-24)
-------------------------
 * Update test database
 * Stop writing msgs_broadcast.text

v8.1.2 (2023-01-24)
-------------------------
 * Stop reading from Broadcast.text

v8.1.1 (2023-01-19)
-------------------------
 * Write new translations JSONB column when saving child broadcasts
 * Remove support for legacy expressions in broadcasts

v8.1.0 (2023-01-18)
-------------------------
 * Update to latest goflow which moves to flow spec version 13.2
 * Tweak fetching contacts eligible for a new campaign event

v8.0.0 (2023-01-09)
-------------------------
 * Update test database to latest schema

v7.5.36 (2023-01-02)
-------------------------
 * Update to latest goflow which adds locale field to MsgOut
 * Improve error reporting when courier call fails

v7.5.35 (2022-12-05)
-------------------------
 * Retry messages which fail to queue to courier

v7.5.34 (2022-11-30)
-------------------------
 * Update deps including new goflow which adds legacy_vars issues
 * Fix test and cleanup msg status constants
 * Add basic auth for Elastic

v7.5.33
----------
 * Test with only redis 6.2
 * Don't filter labels by label_type which is being removed

v7.5.32
----------
 * Send msg id to fetch-attachments endpoint and clean up flows.MsgID vs models.MsgID
 * Revert "Remove rocketchat as ticketer"
 * Don't include unavailable attachments in flows but do save them

v7.5.31
----------
 * Set server idle timeout to 90 seconds
 * Test against redis 6.2 and postgres 14

v7.5.30
----------
 * Add workaround to contact resolve endpoint to deal with invalid phone numbers

v7.5.29
----------
 * Update to latest gocommon and goflow
 * Remove rocketchat as ticketer

v7.5.28
----------
 * Update deps

v7.5.27
----------
 * Remove legacy functionality to look for missed call trigger if there's no incoming call trigger

v7.5.26
----------
 * Update to latest goflow

v7.5.25
----------
 * Update to latest goflow

v7.5.24
----------
 * Update to latest goflow
 * Remove some leftover references to topups

v7.5.23
----------
 * Remove topups

v7.5.22
----------
 * Set is_active=TRUE on new broadcasts

v7.5.21
----------
 * Update to latest goflow

v7.5.20
----------
 * Remove unused created_on field from msg events
 * Rename media config vars
 * Update to latest gocommon

v7.5.19
----------
 * Add channel removed failed reason for msgs
 * Call courier endpoint to fetch attachments if they're raw URLs

v7.5.18
----------
 * Move interrupt_channel task into tasks/interrupts
 * Tweak msg retries to ignore deleted channels
 * Remove no longer used channels option from interrupt_sessions task

v7.5.17
----------
 * Update to latest goflow
 * Add interrupt channel task

v7.5.16
----------
 * If starting a message flow from an ivr trigger, send reject response

v7.5.15
----------
 * Update to latest goflow which changes expirations for dial waits
 * Add support for time limits on dial waits
 * Rework message events to use channel UUID and include channel type
 * Simplify getting active call count
 * Allow incoming call triggers to take message flows

v7.5.14
----------
 * Switch to new ivr_call table

v7.5.13
----------
 * Update test database
 * Rename channelconnection to call
 * Don't read or write connection_type
 * Attach channel logs to channel connections

v7.5.12
----------
 * Add redaction to IVR channel logging

v7.5.11
----------
 * Use MsgOut.UnsendableReason instead of checking contact status
 * Restructure channel logs like how they are in courier
 * Update to latest goflow and gocommon

v7.5.10
----------
 * Bump default steps per sprint limit to 200
 * Use go 1.19

v7.5.9
----------
 * Add channel log UUID field
 * Add codecov token to ci.yml

v7.5.8
----------
 * Update to latest goflow and gocommon

v7.5.7
----------
 * Update to latest gocommon

v7.5.6
----------
 * Update to latest gocommon and goflow
 * Save IVR channel logs in new format
 * Upgrade dependencies

v7.5.5
----------
 * Update to latest goflow and gocommon
 * Add support for AWS Cred Chains

v7.5.4
----------
 * Add test utility to make it easier to test with imported flows
 * Failed sessions should be saved with ended_on set

v7.5.3
----------
 * Add endpoint to do synchronous interrupt of a single contact

v7.5.2
----------
 * Update to latest goflow
 * Set opened_by_id on the open ticket event as well as the ticket itself

v7.5.1
----------
 * Add Ticket.opened_by and opened_in and set when opening tickets

v7.5.0
----------
 * Update to latest goflow with support for ticket modifier
 * Update to latest goflow which removes sessions from engine services

v7.4.1
----------
 * Update to latest goflow

v7.4.0
----------
 * Update README

v7.3.20
----------
 * Use proper query construction for preview_start endpoint and return search errors for invalid user queries

v7.3.19
----------
 * Update dependencies
 * Log version at startup

v7.3.18
----------
 * Use new orgmembership table to load users
 * Update to latest goflow

v7.3.17
----------
 * Update to latest goflow and simplify code for exiting session runs
 * Add support for excluding contacts already in a flow in start_session actions
 * Don't blow up in msg_created handler if flow has been deleted
 * Use analytics package from gocommon instead of librato directly

v7.3.16
----------
 * Update to latest goflow

v7.3.15
----------
 * Simplify BroadcastBatch
 * Record first-reply timings for tickets
 * Add arm64 as a build target

v7.3.14
----------
 * Update to latest goflow which fixes contact query bug

v7.3.13
----------
 * Update to latest goflow which fixes contact query simplification
 * Record ticket daily counts when opening, assigning and replying to tickets
 * Update to latest gocommon, phonenumbers, jsonparser

v7.3.12
----------
 * Update to go 1.18 and use some generics

v7.3.11
----------
 * Rework flow/preview_start endpoint to take a number of days since last seen on
 * Update to latest goflow that has fix for whatsapp template selection

v7.3.10
----------
 * Changes to preview_start endpoint - 1) rename count to total to match other search endpoints, 2) add 
query inspection metadata to preview_start endpoint response 3) switch to UUIDs for contacts and groups

v7.3.9
----------
 * Move search into its own package and add more tests
 * Add endpoint to generate a flow start preview

v7.3.8
----------
 * Use new contactfield.name and is_system fields

v7.3.7
----------
 * Update to latest goflow and start using httpx.DetectContentType

v7.3.6
----------
 * Update modified_on for flow history changes  by handling flow entered and sprint ended 
events

v7.3.5
----------
 * Update to latest goflow which requires mapping groups and flows to ids for ES queries

v7.3.4
----------
 * Fix unstopping of contacts who message in

v7.3.3
----------
 * ContactGroup.group_type can no longer be 'U'
 * Clear session timeout if timeout resume rejected by wait
 * Update golang.org/x/sys

v7.3.2
----------
 * Add is_system to contact groups, filter groups by group_type = M|Q|U

v7.3.1
----------
 * Simplify cron jobs and add them to the main mailroom waitgroup
 * Allow expirations and timeouts to resume sessions for stopped, blocked and archived contacts
 * Messages to stopped, blocked or archived contacts should immediately fail

v7.3.0
----------
 * Update to latest goflow
 * Replace last usages of old locker code
 * Cleanup some SQL variables

v7.2.6
----------
 * Batch calls to delete event fires

v7.2.5
----------
 * Fix resend reponses when not all messages could be resent

v7.2.4
----------
 * Improve logging on session resume
 * Fix example session storage path
 * Use redis 5.x for CI
 * Improve configuration section of README

v7.2.3
----------
 * Rework resending to fail messages with no destination

v7.2.2
----------
 * Tweak log messages for expirations and timeouts
 * Don't try to resume expired session if contact isn't active

v7.2.1
----------
 * Improve logging of expirations task and fix logging on ticket opening
 * CI with latest go 1.17.x

v7.2.0
----------
 * Add missing config options to README

v7.1.45
----------
 * Update to latest goflow

v7.1.44
----------
 * Remove references to flows_flowrun.exit_type and is_active which are no longer used

v7.1.43
----------
 * Flow starts from start_session actions in flows should only match single contacts
 * Fix panic when resuming IVR flow

v7.1.42
----------
 * When fetching flows by name, prefer latest saved

v7.1.41
----------
 * Add support for querying by flow

v7.1.40
----------
 * Implement setting contact.current_flow_id as pseudo event and hook

v7.1.39
----------
 * Change StartOption fields to match excludes that we use in RP UI
 * Skipping or interrupting waiting sessions should happen across all flow types

v7.1.38
----------
 * Update to latest goflow that fixes group removal on contact stop and resuming with wrong type of resume
 * Resolve endpoint should return error if given invalid URN
 * If handling timed event finds different session, don't fail event session as it should have already been interrupted

v7.1.37
----------
 * Update to goflow 0.149.1
 * Add _import_row to contact spec so that we can reliably generate import errors with row numbers

v7.1.36
----------
 * Remove expires_on, parent_uuid and connection_id fields from FlowRun

v7.1.35
----------
 * Use sessions only for voice session expiration
 * FlowSession.wait_resume_on_expire now non-null and don't set to true for IVR flows

v7.1.34
----------
 * Update modified_on for contacts in batches of 100
 * Rework expiring messaging sessions to be session based

v7.1.33
----------
 * Set wait fields on sessions for dial waits as well
 * Create completed sessions with wait_resume_on_expire = false
 * Reduce exit sessions batch size to 100
 * Clear contact.current_flow_id when exiting sessions

v7.1.32
----------
 * Rework expirations to use ExitSessions

v7.1.31
----------
 * Consolidate how we interrupt sessions
 * Tweak mailroom shutdown to only stop ES client if there is one

v7.1.30
----------
 * Remove deprecated fields on search endpoint
 * Include flow reference when queuing messages
 * Tweak coureier payload to not include unused fields

v7.1.29
----------
 * Update to latest goflow (fixes allowing bad URNs in start_session actions and adds @trigger.campaign)
 * Commit modified_on changes outside of transaction

v7.1.28
----------
 * Include redis stats in analytics cron job
 * Update wait_resume_on_expire on session writes

v7.1.27
----------
 * Always read run status instead of is_active
 * Rename Session.TimeoutOn to WaitTimeoutOn
 * Add flow_id to msg and record for flow messages

v7.1.26
----------
 * Add testdata functions for testing campaigns and events
 * Use models.FireID consistently
 * Replace broken redigo dep version and anything that was depending on it
 * Simplify how we queue event fire tasks and improve logging

v7.1.25
----------
 * Update to latest gocommon
 * Stop writing events on flow runs

v7.1.24
----------
 * Switch to dbutil package in gocommon
 * Always exclude router arguments from PO file extraction

v7.1.23
----------
 * Session.CurrentFlowID whould be cleared when session exits
 * Start writing FlowSession.wait_expires_on
 * Update to latest goflow which removes activated waits
 * Clamp flow expiration values to valid ranges when loading flows

v7.1.22
----------
 * Replace redisx package with new dependency
 * Update test database to use big ids for flow run and session ids
 * Move session storage mode to the runtime.Config instead of an org config value

v7.1.21
----------
 * Update to latest gocommon to get instagram scheme

v7.1.20
----------
 * Update to latest gocommon and goflow to get fix for random.IntN concurrency

v7.1.19
----------
 * Update to latest goflow

v7.1.18
----------
 * Fix not logging details of query errors
 * CI with go 1.17.5

v7.1.17
----------
 * Include segments in simulation responses

v7.1.16
----------
 * Record recent contacts for all segments
 * Allow cron jobs to declare that they can run on all instances at same time - needed for analytics job
 * Write failed messages when missing channel or URNs
 * Switch to redisx.Locker for cron job locking
 * Update goflow
 * Rename redisx structs and remove legacy support from IntervalSet

v7.1.15
----------
 * Update goflow
 * Use new key format with redisx.Marker but also use legacy key format for backwards compatibility

v7.1.14
----------
 * Update to latest goflow
 * Add failed_reason to msg and set when failing messages due to looping or org suspension
 * Simplify cron functions by not passing lock name and value which aren't used
 * Stop writing msgs_msg.connection_id
 * Stop writing msgs_msg.response_to

v7.1.13
----------
 * Replace trackers with series to determine unhealthy webhooks
 * Correct use of KEYS vs ARGV in redisx scripts
 * Rework how we create outgoing messages, and fix retries of high priority messages

v7.1.12
----------
 * Move msg level loop detection from courier

v7.1.11
----------
 * Add imports for missing task packages

v7.1.10
----------
 * Add redisx.Cacher util

v7.1.9
----------
 * Don't include response_to_id in courier payloads
 * Add logging for ending webhook incidents

v7.1.8
----------
 * Update sessions and runs in batches when exiting

v7.1.7
----------
 * Fix handling of add label actions after msg resumes in IVR flows
 * Add cron job to end webhook incidents when nodes are no longer unhealthy
 * Re-add new locker code but this time don't let locking code hold redis connections for any length of time
 * Create incident once org has had unhealthy webhooks for 20 minutes

v7.1.6
----------
 * Revert "Rework locker code for reusablity"

v7.1.5
----------
 * Pin to go 1.17.2

v7.1.4
----------
 * Rework redis marker and locker code for reusablity
 * Test with Redis 3.2.4
 * Add util class to track the state of something in redis over a recent time period
 * Remove unneeded check for RP's celery task to retry messages

v7.1.3
----------
 * Add logging to msg retry task

v7.1.2
----------
 * Add task to retry errored messages

v7.1.1
----------
 * Remove notification.channel_id

v7.1.0
----------
 * Update to latest goflow with expression changes
 * Make LUA script to queue messages to courier easier to understand
 * Explicitly exclude msg fields from marshalling that courier doesn't use
 * Remove unused code for looking up msgs by UUID

v7.0.1
----------
 * Update to latest goflow

v7.0.0
----------
 * Tweak README

v6.5.43
----------
 * Update to latest goflow which adds reverse function

v6.5.42
----------
 * Change default resumes per session limit from 500 to 250
 * Update to latest goflow

v6.5.41
----------
 * Update to latest goflow which adds sort function

v6.5.40
----------
 * Add config option for maximum resumes per session

v6.5.39
----------
 * Add notification.email_status

v6.5.38
----------
 * Update to latest goflow which simplifies contactql queries after parsing
 * Load contacts for flow starts from readonly database
 * CI testing on PG12 and 13

v6.5.37
----------
 * Look for From param if Caller param not found in incoming IVR call request
 * Update to latest gocommon and go 1.17

v6.5.36
----------
 * Drop ticket.subject
 * Remove no longer used FlowStart.CreatedBy

v6.5.35
----------
 * Tweak mailroom startup to show warning if no distinct readonly DB configured

v6.5.34
----------
 * Switch to readonly database for asset loading

v6.5.33
----------
 * Add support for READONLY_DB config setting that opens a new readonly database connection
 * Finish the runtime.Runtime refactor

v6.5.32
----------
 * Refactor more code to use runtime.Runtime instead of passing db instances and using the global config
 * Update to latest goflow with doc fixes

v6.5.31
----------
 * Recalculate dynamic groups after closing and reopening tickets
 * Stop writing webhook results

v6.5.30
----------
 * Fix handling webhook called events on resumes

v6.5.29
----------
 * Add new fields to HTTPLog and save for webhook called events
 * Stop writing ticket subjects

v6.5.28
----------
 * Add warning log entry when task takes longer than 1 minute

v6.5.27
----------
 * Update to latest goflow (fixes word_slice)

v6.5.26
----------
 * Update to latest goflow

v6.5.25
----------
 * Add notifications for contact imports and set contact import status

v6.5.24
----------
 * Fix IVR for orgs using S3 sessions
 * Ticket notifications (opened and activity)

v6.5.23
----------
 * Add force param to close tickets endpoint to let us ignore errors on external ticket service when removing a ticketer

v6.5.22
----------
 * Support Spanish status names passed back from Zendesk targets

v6.5.21
----------
 * Read sessions from db or s3 depending on whether output_url is set
 * Don't write output in db when writing to s3

v6.5.20
----------
 * Add endpoint to change ticket topic
 * Update to latest goflow/gocommon/phonenumbers

v6.5.19
----------
 * Update to latest goflow to get ticket topic changes there
 * Add ticket topics

v6.5.18
----------
 * Switch to synchronous answering machine detection for Twilio channels

v6.5.15
----------
 * Make IVR machine detection an option in channel config

v6.5.14
----------
 * Stop reading/writing channelconnection.retry_count so that it can be dropped

v6.5.13
----------
 * Don't let IVR status callbacks overwrite error status (otherwise calls aren't retried)

v6.5.12
----------
 * Revert previous to query to fetch calls to retry so we only look as statuses Q and E
 * Add ChannelConnection.errorReason and start populating
 * Reinstate channel connection error_count and write it
 * Fix retrying of calls where answering machine was detected

v6.5.11
----------
 * Implement asynchronous AMD for Twilio IVR
 * Enable answering machine detection for Vonage IVR

v6.5.10
----------
 * Fix requests to twilio to start calls with machine detection

v6.5.9
----------
 * Add support for answering machine detection on Twilio calls
 * Update to latest goflow

v6.5.8
----------
 * Use new config keys for LUIS classifiers

v6.5.7
----------
 * Switch from abandoned dgrijalva/jwt-go to golang-jwt/jwt
 * Update to latest goflow (adds support for queries on tickets, fixes LUIS classifiers)

v6.5.6
----------
 * Update to latest goflow and add parse_only as param to parse_query to allow us to extract field dependencies even when they don't yet exist in the database

v6.5.5
----------
 * Fix tests broken by recent db changes to msgs and broadcasts
 * Populate ticket_count when creating new contacts

v6.5.4
----------
 * Actually save IVR messages with sent_on set

v6.5.3
----------
 * Update contact modified_on after populate dynamic group task
 * Update to latest goflow

v6.5.2
----------
 * Set sent_on for outgoing IVR messages

v6.5.1
----------
 * Support flow config ivr_retry values of -1 meaning no retry
 * Log error if marking event fire as fired fails

v6.5.0
----------
 * Update to latest goflow and gocommon

v6.4.3
----------
 * Fix triggering new IVR flow from a simulation resume so that it includes connection to test channel

v6.4.2
----------
 * Latest goflow with latest localization

v6.4.1
----------
 * Update to latest goflow to get fixes for nulls in webhook responses
 * Add new error type for failed SQL queries

v6.4.0
----------
 * move s3 session config error to a warning for the time being since not strictly required yet

v6.3.31
----------
 * Support ticket open events with assignees
 * Add endpoints for ticket assignment and adding notes

v6.3.30
----------
 * Update to latest goflow

v6.3.29
----------
 * Include args in BulkQuery error output

v6.3.28
----------
 * Return more SQL when BulkQuery errors
 * Update to latest goflow/gocommon

v6.3.27
----------
 * Fix handling of inbox messages to also update open tickets

v6.3.26
----------
 * Stop writing broadcast.is_active which is now nullable

v6.3.25
----------
 * Update to latest goflow

v6.3.24
----------
 * Update to latest goflow
 * Load org users as assets and use for ticket assignees and manual trigger users
 * Add ticket to broadcasts and update last_activity_on after creating messages for a broadcast with a ticket

v6.3.23
----------
 * Add support for exclusion groups on scheduled triggers

v6.3.22
----------
 * Update ticket last_activity_on when opening/closing and for incoming messages
 * Set contact_id when creating new tickets events

v6.3.21
----------
 * Update to latest goflow and which no longer takes default_language

v6.3.20
----------
 * Have our session filename lead with timestamp so other objects can exist in contact dirs

v6.3.19
----------
 * Parse URL to get path out for sessions

v6.3.18
----------
 * Use s3 session prefix when building s3 paths, default to /
 * Throw error upwards if we have no DB backdown
 * Read session files from storage when org configured to do so

v6.3.17
----------
 * Ignore contact tickets on ticketers which have been deleted

v6.3.16
----------
 * Add ticket closed triggers and use to handle close ticket events
 * Add ticket events and insert when opening/closing/reopening tickets

v6.3.15
----------
 * Fix test which modifies org
 * Update to latest goflow as last release was broken

v6.3.14
----------
 * Update to latest goflow
 * Write sessions to s3 on resumes (optionally)
 * Add support for exclusion groups on triggers and generalize trigger matching

v6.3.13
----------
 * Introduce runtime.Runtime
 * Simplify testdata functions
 * Various fixes from linter
 * Simplify use of test contacts in handler tests
 * Move test constants out of models package
 * Remove reduntant resend_msgs task

v6.3.12
----------
 * Update to latest goflow (legacy_extra is no longer an issue)
 * Make Msg.next_attempt nullable
 * Add web endpoint for msg resends so they can be a synchronous operation

v6.3.11
----------
 * Expose open tickets as @contact.tickets

v6.3.9
----------
 * Fix queueing of resent messages to courier and improve testing of queueing
 * Update to latest goflow
 * Add WA template translation namespace

v6.3.8
----------
 * Add task to resend messages

v6.3.7
----------
 * Update to latest goflow
 * Update test database and rename Nexmo to Vonage

v6.3.6
----------
 * Update to latest goflow

v6.3.5
----------
 * Update to latest goflow

v6.3.4
----------
 * Update to latest goflow
 * Smarter loading for cache misses on org assets

v6.3.3
----------
 * Update to latest goflow

v6.3.2
----------
 * Simplify caching, keep orgs for 5s, reload everything

v6.3.1
----------
 * Update to latest goflow

v6.3.0
----------
 * Fail expirations that are no longer the active session

v6.2.3
----------
 * Update to latest goflow with completed es and pt-BR translations

v6.2.2
----------
 * Update to latest goflow

v6.2.1
----------
 * use SaveRequest() so our ivr logs always have bodies

v6.2.0
----------
 * 6.2.0 Release Candidate

v6.2.0
----------
 * 6.2.0 Release Candidate

v6.1.19 
----------
 * Fix campaign even firing for background flows
 * IVR forwards for Nexmo and Twilio

v6.1.18
----------
 * Update to latest goflow
 * Rename tickets/internal package

v6.1.17
----------
 * Should match referral trigger with case insensitive

v6.1.16
----------
 * Update to latest goflow
 * Add link local ips to default disallowed networks config

v6.1.15
----------
 * Update phonenumbers lib
 * Decrease locations cache timeout to 1 minute

v6.1.14
----------
 * Support ElasticSearch 7.2 (backwards incompatible to Elastic 6.*)

v6.1.13
----------
 * Update to latest goflow

v6.1.12
----------
 * Update to latest goflow v0.110.0

v6.1.11
----------
 * Update to latest goflow v0.109.4

v6.1.10
----------
 * Simplify FCM client code
 * Fix updating message status when id column is bigint
 * Ensure courier messages are always queued for a single contact
 * Fix not triggering FCM syncs for broadcasts and ticket reply messages

v6.1.9
----------
 * Update to goflow v0.109.0

v6.1.8
----------
 * Update to latest goflow 0.108.0

v6.1.7
----------
 * Use background instead of passive

v6.1.6
----------
 * Update to latest goflow v0.107.2
 * Add support for passive flows

v6.1.5
----------
 * Update to goflow v0.107.1

v6.1.4
----------
 * Variable timeout for fire campaign task

v6.1.3
----------
 * Fix misreporting created contacts as updated during imports

v6.1.2
----------
 * Ensure field and group assets used for imports are fresh
 * Add support for internal type ticketers

v6.1.1
----------
 * Update to latest goflow v0.106.3

v6.1.0
----------
 * Configure engine to disallow HTTP requests to private networks

v6.0.3
----------
 * correct name for completion.json in release

v6.0.2
----------
 * pin goreleaser, explicit inclusion of /docs/*, completions and functions

v6.0.1
----------
 * add log when queuing to courier

v6.0.0
----------
 * Update test database

v5.7.44
----------
 * Add ticket service for Rocket.Chat

v5.7.43
----------
 * Update to latest goflow v0.106.1

v5.7.42
----------
 * Prevent importing invalid URNs during import

v5.7.41
----------
 * Update to latest goflow v0.106.0
 * Don't write to flows_flowrun.timeout_on so it can be dropped

v5.7.40
----------
 * Update to goflow v0.105.5

v5.7.39
----------
 * Fix input_labels_added event handling when session input ID isn't set

v5.7.38
----------
 * Always request Mp3 files from Twilio IVR recordings

v5.7.37
----------
 * Add support for outgoing mailgun ticket attachments

v5.7.36
----------
 * Incoming attachments for mailgun ticketers

v5.7.33
----------
 * Update to goflow v0.105.4
 * Fix cloning of flows during simulation so that ignore keyword triggers is cloned too

v5.7.32
----------
 * Update to goflow v0.105.3 to get support for arabic numerals in has_number tests

v5.7.31
----------
 * Update to latest goflow to get for normalizing numbers from The Gambia
 * Enable retrying on the elastic client

v5.7.30
----------
 * Reorganization of core packages

v5.7.29
----------
 * Fix race condition when bulk getting/creating contacts

v5.7.28
----------
 * Add contact/resolve endpoint to assist with channel events still handled in RP

v5.7.27
----------
 * If a flow start task creates new contacts, save those back to the start

v5.7.26
----------
 * Add mockable DB to enable testing database errors
 * CreateContact also should do lookup before trying to create new contact with URNs
 * Imports 2.0

v5.7.25
----------
 * Pass org from the base task to task structs to remove need for duplicating it in the task body

v5.7.24
----------
 * Add SessionStatus to messages queued to courier

v5.7.23
----------
 * Make defining new task types easier and drier
 * Better locking when handling
 * Fix and simplify creation of channel logs in IVR handlers

v5.7.22
----------
 * Update to latest goflow v0.104.1

v5.7.21
----------
 * Simplify test-smtp cmd using smtpx package
 * Create new dbutil package with generic DB stuff
 * Add task to calculate fires for new campaign event

v5.7.20
----------
 * Fix incoming attachments from Zendesk

v5.7.19
----------
 * Update to latest goflow
 * Empty contact names and languages should be saved as NULL
 * Delete no longer used utils/celery package

v5.7.18
----------
 * Update to latest goflow
 * Add support for incoming attachments on ticketing services

v5.7.17
----------
 * Use status for elastic queries that need to filter out non-active contacts

v5.7.16
----------
 * Add support for excluding contacts from searches by ids
 * Rework utils/storage to be generic and moveable to gocommon

v5.7.15
----------
 * Add create contact endpoint which uses modifiers to add fields and groups to contacts
 * Rework contact creation functions to support creation with multiple URNs

v5.7.14
----------
 * Stop writing is_blocked and is_stopped

v5.7.13
----------
 * Read from contact.status intead of is_stopped/is_blocked
 * Implement saving of zendesk ticket files as attachments
 * Abstract S3 code so tests and dev envs can use file storage

v5.7.12
----------
 * Fix inserting channel logs and add test

v5.7.11
----------
 * Always write contact.status when writing is_blocked or is_stopped
 * Convert IVR code to use goflow's httpx package

v5.7.10
----------
 * Tweak goreleaser config to include subdirectories inside docs folder

v5.7.9
----------
 * Update to goflow v0.101.2
 * Bundle localized goflow docs in release

v5.7.8
----------
 * Recalculate event fires for campaign events based on last_seen_on

v5.7.7
----------
 * Update to latest goflow v0.100.0

v5.7.6
----------
 * Remove protection for overwriting last_seen_on with older values

v5.7.5
----------
 * Update last_seen_on when handling certain channel events
 * Update last_seen_on when we receive a message from a contact

v5.7.4
----------
 * Fail outgoing messages for suspended orgs
 * Refresh groups as well as fields for contact query parsing

v5.7.3
----------
 * Update to goflow v0.99.0

v5.7.2
----------
 * Update to latest goflow v0.98.0
 * Render rich errors with code and extra field

v5.7.1
----------
 * Update to latest goflow v0.96.0
 * Add loop protection by passing session history to new flow action triggers

v5.7.0
----------
 * Set user and origin on manual triggers
 * Switch to trigger builders

v5.6.1
----------
 * expire runs that have no session, just warn while doing so

v5.6.0
----------
 * 5.6.0 Release Candidate

v5.5.38 
----------
 * Varible naming consistency

v5.5.37
----------
 * Fix reading of modifiers so always ignore modifier that becomes noop

v5.5.36
----------
 * Sead country from templates
 * Ignore missing assets when reading modifiers
 * Fail flow starts which can't be started

v5.5.35
----------
 * Update to latest goflow and add tests for field modifiers

v5.5.34
----------
 * Fix detaching URNs

v5.5.33
----------
 * Update to latest goflow v0.93.0

v5.5.32
----------
 * When blocking contacts archive any triggers which only apply to them

v5.5.31
----------
 * Messages without topups should be queued
 * Continue handling as normal for suspended orgs

v5.5.30
----------
 * Org being suspended should stop message handling
 * Make decrementing org credit optional

v5.5.29
----------
 * Return query inspection results as new metadata field in responses
 * Update to latest goflow v0.92.0

v5.5.28
----------
 * Don't do any decoration of email ticket subjects

v5.5.27
----------
 * Allow searching by UUID, as well != matches on ID and UUID
 * Update to latest goflow v0.91.1 to fix clearing fields
 * Maybe fix intermittently failing test

v5.5.26
----------
 * Update to goflow v0.89.0

v5.5.25
----------
 * Add endpoint to change a flow language

v5.5.24
----------
 * Tickets fixes and improvements
 * Update to goflow v0.87.0

v5.5.17
----------
 * Send email when reopening mailgun ticket

v5.5.16
----------
 * Implement closing of tickets in zendesk from mailroom

v5.5.15
----------
 * Send close notification emails in mailgun tickets
 * Reply back to users who replies aren't permitted to go to the ticket
 * Simplify sharing of HTTP configuration between services
 * Add simulator ticket service to fake creating tickets
 * Fix verifying sender when receiving mailgun reply

v5.5.14
----------
 * Basic handling of tickets (mailgun and zendesk ticketer types)

v5.5.13
----------
 * Update to goflow v0.85.0
 * Use go 1.14 and do some minor dep updatse
 * Bump max request bytes to 32MB for web server
 * Implement hooks for status modifier events

v5.5.12
----------
 * Update to goflow v0.83.1

v5.5.11
----------
 * Update to goflow v0.83.0
 * Don't blow up if flow is deleted during simulation

v5.5.10
----------
 * Update to goflow v0.82.0
 * Populate flows_flowstart.start_type

v5.5.9
----------
 * Set org on new flow starts
 * Allow for seeded UUID generation in testing environments with -uuid-seed switch
 * Set language attribute from ivr_created_event on TwiML say action

v5.5.8
----------
 * Update flow start modified_on when making changes
 * Add method to set modified_by on contacts

v5.5.7
----------
 * Update to latest goflow v0.81.0

v5.5.6
----------
 * Update to latest goflow v0.79.1

v5.5.5
----------
 * Update mailroom test db
 * Only Prometheus group can access metrics

v5.5.4
----------
 * Drop no longer used validate_with_org_id param on inspect endpoint
 * Add endpoints to export and import PO files from flows

v5.5.3
----------
 * Add decode_html Excellent function

v5.5.2
----------
 * Change to using basic auth for org specific prom metrics endpoint

v5.5.1
----------
 * Update to latest goflow v0.78.0

v5.5.0
----------
 * Add prometheus endpoint for org level metrics

v5.4.4
----------
 * Update to latest goflow v0.77.4

v5.4.3
----------
 * Update to goflow v0.77.1

v5.4.2
----------
 * Add noop handler for failure events
 * Update to latest goflow v0.77.0

v5.4.1
----------
 * Fix @legacy_extra NPE on router operands

v5.4.0
----------
 * Touch readme for 5.4 release

v5.3.44
----------
 * Update to goflow v0.76.2

v5.3.43
----------
 * Update to goflow v0.76.1

v5.3.42
----------
 * Contact search endpoint should also return whether query can be used as group

v5.3.41
----------
 * Update to goflow v0.76.0
 * Add support for searching by group

v5.3.40
----------
 * Update to goflow v0.74.0

v5.3.39
----------
 * Update to goflow v0.72.2
 * Add modify contact endpoint
 * Refactor hooks to allow session-less use

v5.3.38
----------
 * Update to goflow v0.72.0

v5.3.37
----------
 * Better asset caching
 * Convert flow and expression endpoint testing to be snapshot based

v5.3.36
----------
 * allow globals with empty values

v5.3.35
----------
 * Update to goflow v0.71.2

v5.3.34
----------
 * Read allow_international from channel config
 * Add elastic query to parse query responses
 * Update to goflow v0.71.1

v5.3.33
----------
 * add != operator for numbers, dates, created_on

v5.3.32
----------
 * Update to goflow v0.70.0

v5.3.31
----------
 * Add urn =, !=, ~ support
 * Fix name sorting on queries
 * Update to latest goflow v0.69.0

v5.3.30
----------
 * Sort locations fields by keyword value
 * Update to latest goflow

v5.3.29
----------
 * Fix > query on numbers, never use cached orgs
 * Update gocommon for v1.2.0
 * Remove superfulous legacy_definition fields on endpoints

v5.3.28
----------
 * Allow dynamic group population within mailroom

v5.3.27
----------
 * Update to latest goflow v0.67.0

v5.3.26
----------
 * Update to goflow v0.66.3

v5.3.25
----------
 * Update to latest goflow

v5.3.24
----------
 * Update to latest goflow v0.66.0

v5.3.23
----------
 * Update to latest goflow v0.65.0

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

