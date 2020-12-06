# Kafka broker

Two services consume messages from kafka topic and send them to api.

## Main service

1) Consumes messages from topic 
2) tries to send them to api
3) produce them to retry topic if not succeed in the following format:

{

    Content: "message_content",
    Tries: 5,
    Ticks: 1234567890

},
where:

Content - message content; Tries - number of tries from CommonConfig; Ticks - number of ticks which represents timestamp of last attempt.

## Retry service

1) Get messages from retry topic in above format
2) tries to send content to api
3) decrease tries counter and update ticks if not succeed
4) produce to retry.
If tries counter equals 0, attempts are exhausted and message is sent to dead topic.

## InfinityRetryMode

Produce message to dead immediately in case when API status code 400;
Retry endlessly on API server error 500.