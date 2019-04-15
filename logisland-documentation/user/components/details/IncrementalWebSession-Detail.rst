This processor creates and updates web-sessions based on incoming web-events. Note that both web-sessions and web-events are stored in elasticsearch.
 Firstly, web-events are grouped by their session identifier and processed in chronological order.
 Then each web-session associated to each group is retrieved from elasticsearch.
 In case none exists yet then a new web session is created based on the first web event.
 The following fields of the newly created web session are set based on the associated web event: session identifier, first timestamp, first visited page. Secondly, once created, or retrieved, the web session is updated by the remaining web-events.
 Updates have impacts on fields of the web session such as event counter, last visited page,  session duration, ...
 Before updates are actually applied, checks are performed to detect rules that would trigger the creation of a new session:

	the duration between the web session and the web event must not exceed the specified time-out,
	the web session and the web event must have timestamps within the same day (at midnight a new web session is created),
	source of traffic (campaign, ...) must be the same on the web session and the web event.

 When a breaking rule is detected, a new web session is created with a new session identifier where as remaining web-events still have the original session identifier. The new session identifier is the original session suffixed with the character '#' followed with an incremented counter. This new session identifier is also set on the remaining web-events.
 Finally when all web events were applied, all web events -potentially modified with a new session identifier- are save in elasticsearch. And web sessions are passed to the next processor.

WebSession information are:
- first and last visited page
- first and last timestamp of processed event
- total number of processed events
- the userId
- a boolean denoting if the web-session is still active or not
- an integer denoting the duration of the web-sessions
- optional fields that may be retrieved from the processed events

