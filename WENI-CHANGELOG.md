1.4.13-mailroom-7.1.22
----------
 * Add Domain to File URL for Zendesk #105

1.4.12-mailroom-7.1.22
----------
 * Fix file endpoint for Zendesk #103

1.4.11-mailroom-7.1.22
----------
 * Add support for sending contact language in messages for WAC and WA #101

1.4.10-mailroom-7.1.22
----------
 * Fix submitting tags and custom fields for Zendesk tickets #99

1.4.9-mailroom-7.1.22
----------
 * add contact urn field to wenichats room creation params #97

1.4.8-mailroom-7.1.22
----------
 * Fix twilio flex messages history #95

1.4.7-mailroom-7.1.22
----------
 * Fix twilio flex media creation content-type param #93

1.4.6-mailroom-7.1.22
----------
 * Fix tag registration, custom_fields and ticket closing in Zendesk #91

1.4.5-mailroom-7.1.22
----------
 * Add Ticket Fields for Zendesk #86
 * twilio flex detect and setup media on create media type  #87
 * twilio flex open ticket can set preferred flexflow from body json field flex_flow_sid #88
 * Swap targets for webhooks in Zendesk #89

1.4.4-mailroom-7.1.22
----------
 * wenichats open ticket with contact fields as default in addition to custom fields

1.4.3-mailroom-7.1.22
----------
 * fix twilio flex contact echo msgs from webhook

1.4.2-mailroom-7.1.22
----------
 * twilio flex support extra fields
 * twilio flex has Header X-Twilio-Webhook-Enabled=True on send msg

1.4.1-mailroom-7.1.22
----------
 * wenichats ticketer support custom fields

1.4.0-mailroom-7.1.22
----------
 * Add wenichats ticketer integration

1.3.3-mailroom-7.1.22
----------
 * Fix contacts msgs query

1.3.2-mailroom-7.1.22
----------
* Replace gocommon v1.16.2 with version v1.16.2-weni compatible with Teams channel

1.3.1-mailroom-7.1.22
----------
 * Replace gocommon for one with slack bot channel urn

1.3.0-mailroom-7.1.22
----------
 * Merge nyaruka tag v7.1.22 into weni 1.2.1-mailroom-7.0.1 and resolve conflicts.

1.2.1-mailroom-7.0.1
----------
 * Tweak ticketer Twilio Flex to allow API key authentication

1.2.0-mailroom-7.0.1
----------
 * Add ticketer Twilio Flex

1.1.0-mailroom-7.0.1
----------
 * Update gocommon to v1.15.1

1.0.0-mailroom-7.0.1
----------
 * Update Dockerfile to go 1.17.5
 * Fix ivr cron retry calls
 * More options in "wait for response". 15, 30 and 45 seconds
 * Support to build Docker image
