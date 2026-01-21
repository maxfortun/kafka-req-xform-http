# kafka-req-xform-http


### Build
Must set `KAFKA_VERSION` for build to work.
```
KAFKA_VERSION=3.7.2-SNAPSHOT ./gradlew jar
```
or
```
KAFKA_VERSION=4.0.0-SNAPSHOT ./gradlew jar
```

### Request Headers
All headers have a prefix of `plugin prefx`-`broker`-.  

|Name|Default|Values|Effect|
|---|---|---|---|
|uri|||Uri of the service to forward requests to. Overrides default.|
|enable|true|true,false|Is forwarding to service enabled?|
|headers.in||env,time,timespan,hostname|Include in response headers|
|httpClient.onException|fail|fail,pass-thru,original|HTTP response error handling behavior|
|onException|throw|throw,headers,original,dlq|Transform exception handling behavior|
|onException.dlqTopic|{topic}-dlq||Dead-letter queue topic name (used when onException=dlq)|

#### onException Values

| Value | Behavior |
|-------|----------|
| `throw` | Throws `InvalidRequestException`, breaks connection (default) |
| `headers` | Returns record with error info in headers to original topic, connection stays open |
| `original` | Returns original record unchanged, transformation skipped |
| `dlq` | Routes failed record to dead-letter queue topic with error headers |

When `onException=headers` or `onException=dlq`, the following headers are added to the record:
- `{headerPrefix}error` = `"true"`
- `{headerPrefix}error-class` = exception class name (e.g., `java.net.ConnectException`)
- `{headerPrefix}error-message` = exception message (if present)

When `onException=dlq`, additionally:
- `{headerPrefix}original-topic` = original topic name the record was destined for
- Record is routed to the DLQ topic instead of the original topic
- DLQ topic defaults to `{originalTopic}-dlq`, configurable via `onException.dlqTopic`

### Sample config
```
enable=true
enable.values=(?i)^(true|false)$
enable.scopes=(?i)^(app|request)$

enable-send=true
enable-send.values=(?i)^(true|false)$
enable-send.scopes=(?i)^(app|request)$

uri=$CONTENT_LAKE_URL
uri.scopes=(?i)^(app|request)$

topics.namePattern=(?i)^(?!__).*$
topics.namePattern.scopes=(?i)^(app)$

httpClient.onException=fail
httpClient.onException.values=(?i)^(fail|pass-thru|original)$
httpClient.onException.scopes=(?i)^(app|request)$

onException=throw
onException.values=(?i)^(throw|headers|original|dlq)$
onException.scopes=(?i)^(app|request)$

onException.dlqTopic=
onException.dlqTopic.scopes=(?i)^(app|request)$

httpHeaderPrefix=cl-brk-
httpHeaderPrefix.scopes=(?i)^(app)$

headers.res=(?i)^(env|time|timespan|hostname)$
headers.res.scopes=(?i)^(app|request)$

headers.http=cl-api-header-prefix=cl-api-

map=false
map.values=(?i)^(true|false)$
map.scopes=(?i)^(app)$

map-broker=localhost:9092
map-broker.values=(?i)^(.*:[0-9]*)$
map-broker.scopes=(?i)^(app)$

map-topic=__lineage
map-topic.values=(?i)^(__.*)$
map-topic.scopes=(?i)^(app)$

```

