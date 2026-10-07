# kafka-req-xform-http


### Build
Must set `KAFKA_VERSION` for build to work.
```
export JAVA_HOME=$(/usr/libexec/java_home -v 17.0.16)
KAFKA_VERSION=3.7.2-SNAPSHOT ./gradlew jar
```
or
```
KAFKA_VERSION=4.0.0-SNAPSHOT ./gradlew jar
```

### Request Headers
All headers have a prefix of `plugin prefix`-`broker`-, configurable with `headers.prefix`.

`headers.prefix` can be a comma separated list, e.g. `headers.prefix=cl-brk-,content-lake-broker-`. This is useful when moving to a new prefix:
- For each setting, the prefixes are tried in order and the first header found is used. So `cl-brk-enable` wins over `content-lake-broker-enable`.
- Headers with any of the prefixes are treated as broker headers: they are not forwarded to the service, and they are removed from service responses.
- Headers the plugin writes (e.g. `{prefix}hostname`, `{prefix}error`, `{prefix}req-time`) use only the first prefix.

|Name|Default|Values|Effect|
|---|---|---|---|
|uri|||Uri of the service to forward requests to. Overrides default.|
|enable|true|true,false|Is forwarding to service enabled?|
|headers.in||env,time,timespan,hostname|Include in response headers|
|headers.recordKey|kafka.KEY||HTTP header name for the record key. If set, uses original casing from request headers if present.|
|httpClient.onException|fail|fail,pass-thru,original|HTTP response error handling behavior|
|onException|throw|throw,headers,original,dlq|Transform exception handling behavior|
|onException.dlqTopic|{topic}-dlq||Dead-letter queue topic name (used when onException=dlq)|

### HTTP Request Headers (sent to service)

The following headers are automatically included in HTTP requests to the downstream service:

| Header | Description |
|--------|-------------|
| `{prefix}hostname` | Broker hostname (from HOSTNAME env var) |
| `{prefix}topic-name` | Kafka topic name |
| `{prefix}partition-index` | Partition index |
| `{prefix}record-offset` | Record offset within the partition |
| `{prefix}req-time` | Request timestamp (epoch millis) |
| `kafka.KEY` (or configured name) | Record key (if present). Header name is configurable via `headers.recordKey`; if configured and a matching header exists in the request (case-insensitive), uses the original casing. |

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

headers.logKey=logKey
headers.logKey.scopes=(?i)^(app|request)$

headers.recordKey=kafka.KEY
headers.recordKey.scopes=(?i)^(app|request)$

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

---

## Environment Variables

Transformers do not have a fixed list of environment variables. Instead, **every config key** can be overridden per transformer from the environment. On top of that, a few environment variables are read directly.

### Config Resolution Order

For a transformer named `{name}` (the name used in `TransformingProduceRequestParser.properties`, e.g. `content-lake`), the value of config key `{key}` is resolved as follows. The first value found wins:

| # | Source | Name | Example (`content-lake`, key `uri`) |
|---|---|---|---|
| 1 | Record header (only if `{key}.scopes` matches `request`) | `{headerPrefix}{key}` with non `[a-zA-Z0-9-]` chars replaced by `-`; each prefix in `headers.prefix` is tried in order | `content-lake-broker-uri` |
| 2 | JVM system property | `{name}-{key}` | `-Dcontent-lake-uri=http://...` |
| 3 | Environment variable | `{name}_{key}`, with `.` and `-` in `{name}` replaced by `_` | `content_lake_uri=http://...` |
| 4 | Properties file on the classpath | `{name}.properties` → `{key}` | `content-lake.properties` → `uri=http://...` |

Notes:
- Only the transformer **name** is normalized. The **key** is used exactly as written, including case, dots and dashes. For example `httpClient.soTimeout` becomes `content_lake_httpClient.soTimeout`, and `enable-send` becomes `content_lake_enable-send`. Most shells can't `export` names like these, but you can set them with `env`, `docker run -e`, or a Kubernetes `env:` entry.
- An empty value is still a value. For example, `content_lake_enable=` (empty) stops the lookup and hides the properties file value. It is then treated as "not configured".
- `{key}.scopes` goes through the same lookup, from step 2 onwards. So the environment can also turn request-header overrides on or off, e.g. `content_lake_enable.scopes=(?i)^(app)$`.
- Keys with no default must resolve somewhere. Otherwise the transform throws `IllegalArgumentException` (`Required header ... is not specified in request and has no default value.`), which is then handled according to `onException`. For `HttpProduceRequestDataTransformer`, `enable` has no default.

### Overridable Keys (`HttpProduceRequestDataTransformer`)

Env var names are shown for a transformer named `content-lake`.

| Key | Env var | Code default | Effect |
|---|---|---|---|
| `enable` | `content_lake_enable` | none (required) | Pattern matched against `false`; if it matches, the record passes through without an HTTP call |
| `enable-send` | `content_lake_enable-send` | `true` | Send the HTTP request to the service |
| `uri` | `content_lake_uri` | none | Service URI |
| `topics.namePattern` | `content_lake_topics.namePattern` | none (all topics) | Topics to transform |
| `onException` | `content_lake_onException` | `throw` | Transform exception handling, see [onException Values](#onexception-values) |
| `onException.dlqTopic` | `content_lake_onException.dlqTopic` | `__{topic-name}-dlq` | DLQ topic name |
| `httpClient.class` | `content_lake_httpClient.class` | | HTTP client implementation |
| `httpClient.onException` | `content_lake_httpClient.onException` | `fail` | HTTP error handling |
| `httpClient.*` | `content_lake_httpClient.*` | | Timeouts and pool settings, see [AHC5 HTTP Client Configuration](#ahc5-http-client-configuration) |
| `headers.prefix` | `content_lake_headers.prefix` | `{name}-broker-` | Comma separated prefixes of the plugin's control headers, tried in order. The first one is used for headers the plugin writes |
| `headers.http` | `content_lake_headers.http` | none | Extra `key=value` HTTP headers sent to the service |
| `headers.persistentPattern` | `content_lake_headers.persistentPattern` | none | Response headers kept on the record |
| `headers.transientPattern` | `content_lake_headers.transientPattern` | none | Headers forwarded to the service but not persisted |
| `headers.envPattern` | `content_lake_headers.envPattern` | none | Env vars exposed as response headers (see below) |
| `headers.res` | `content_lake_headers.res` | none | Which of `hostname,env,time,timespan` are added to the response |
| `headers.logKey` | `content_lake_headers.logKey` | none | Record header used as log correlation key |
| `headers.recordKey` | `content_lake_headers.recordKey` | `kafka.KEY` | HTTP header name for the record key |

Each key's `.scopes` companion (e.g. `content_lake_uri.scopes`) can be overridden the same way.

### Environment Variables Read Directly

| Variable | Effect |
|---|---|
| `HOSTNAME` | Sent to the service as `{prefix}hostname`, and added to the response when `headers.res` matches `hostname` |
| Any variable matching `headers.envPattern` | When `headers.res` matches `env`, added to the record as `{prefix}env-{NAME}`, with `_` replaced by `-` |
| `CLASSPATH` | Where `{name}.properties` and `TransformingProduceRequestParser.properties` are found |

---

## AHC5 HTTP Client Configuration

The `AHC5HttpClient` (Apache HttpClient 5) provides configurable connection pooling and timeout settings. All parameters use the `httpClient.` prefix.

### Connection Parameters

| Name | Default | Type | Description |
|------|---------|------|-------------|
| `httpClient.responseTimeout` | Infinite | seconds | Total timeout for entire HTTP request/response cycle |
| `httpClient.soTimeout` | Infinite | seconds | Socket SO_TIMEOUT - max time waiting for data on an established connection |
| `httpClient.socketTimeout` | Infinite | seconds | Socket timeout for connection-level operations |
| `httpClient.connectTimeout` | Infinite | seconds | Timeout for establishing a new connection |
| `httpClient.connTimeToLiveInSeconds` | 10 | seconds | Maximum time a connection can be kept alive in the pool |
| `httpClient.connValidateAfterInactivityInSeconds` | 5 | seconds | Validate connections after this inactivity period before reuse |
| `httpClient.connEvictIdleConnectionsInSeconds` | 10 | seconds | Background eviction of idle connections (runs periodically) |
| `httpClient.closeIdleConnectionsInSeconds` | 30 | seconds | Close idle connections older than this at startup |
| `httpClient.maxConnPerRoute` | 200 | integer | Maximum connections per route (host) |
| `httpClient.maxConnTotal` | 1000 | integer | Maximum total connections in the pool |
| `httpClient.keepAliveTimeoutInSeconds` | 30 | seconds | How long an idle connection is kept alive when the server sends no `Keep-Alive` header |

### Connection Pool Settings

The AHC5 client uses a pooling connection manager with:
- **TCP_NODELAY**: Enabled (reduces latency)
- **SO_KEEPALIVE**: Enabled (detects dead connections)
- **Pool Concurrency Policy**: LAX (allows concurrent access)
- **Pool Reuse Policy**: LIFO (reuses most recently used connections)
- **Background Eviction**: Automatically evicts idle and expired connections

### Sample AHC5 Configuration

```properties
# HTTP client class
httpClient.class=org.apache.kafka.common.requests.transform.AHC5HttpClient

# Timeouts (in seconds)
httpClient.responseTimeout=10
httpClient.soTimeout=60
httpClient.socketTimeout=30
httpClient.connectTimeout=10

# Connection pool, tuned for long-lived connections (see Connection Reuse below)
httpClient.connTimeToLiveInSeconds=600
httpClient.connValidateAfterInactivityInSeconds=5
httpClient.connEvictIdleConnectionsInSeconds=60
httpClient.keepAliveTimeoutInSeconds=60
httpClient.closeIdleConnectionsInSeconds=30
httpClient.maxConnPerRoute=100
httpClient.maxConnTotal=500

# Error handling
httpClient.onException=fail
```

### Connection Reuse

All transformers that use the same `httpClient.class` share one client and one connection pool per JVM. The pool is configured by whichever transformer creates it first, so keep `httpClient.*` settings the same across transformers.

The code defaults close every connection after 10 seconds (`connTimeToLiveInSeconds`), even busy ones, which means a new TCP handshake per pooled connection every 10 seconds. The sample above keeps connections for up to 10 minutes and drops them after 60 seconds idle.

The service's keep-alive timeout must be longer than `connEvictIdleConnectionsInSeconds`. Otherwise the service closes connections the client still considers usable, and requests on them fail. Node.js defaults `server.keepAliveTimeout` to 5 seconds; raise it, e.g. `server.keepAliveTimeout = 65000` and `server.headersTimeout = 66000`.

Both clients use HTTP/1.1 only. HTTP/2 lowercases header names, and the service treats them as case sensitive.

### Pool Statistics Logging

When INFO logging is enabled, the client logs connection pool statistics when the pending connection count changes:
```
connections: { max: 1000, leased: 5, avail: 10, pending: 2 }
```

---

## HttpOffsetFetchResponseDataTransformer

Intercepts OffsetFetch responses and makes HTTP requests for each partition's offset data. This allows external services to be notified of consumer group offset fetches and optionally modify the returned offset data.

### Use Cases
- Audit/track consumer group offset fetches
- Implement custom offset management logic
- Integrate with external monitoring systems
- Dynamic offset manipulation based on external state

### Configuration

| Name | Default | Values | Effect |
|------|---------|--------|--------|
| uri | | | URI of the service to forward offset fetch data to (required) |
| enable | true | true,false | Is transformation enabled? |
| enable-send | true | true,false | Actually send HTTP requests? |
| groups.idPattern | | regex | Filter by consumer group ID (optional) |
| topics.namePattern | | regex | Filter by topic name (optional) |
| httpClient.class | | class name | HTTP client implementation to use |
| httpClient.onException | fail | fail,pass-thru,ignore | HTTP error handling behavior |
| onException | throw | throw,ignore | Transform exception handling |
| headers.http | | key=value pairs | Additional HTTP headers to include |

### HTTP Request Format

**Method:** POST

**Headers:**
- `{prefix}hostname` - Broker hostname
- `{prefix}api-type` - "OffsetFetch"
- `{prefix}topic-name` - Topic name
- `{prefix}partition-index` - Partition index
- `{prefix}committed-offset` - Current committed offset
- `{prefix}committed-leader-epoch` - Leader epoch
- `{prefix}group-id` - Consumer group ID (v8+ format only)
- `{prefix}metadata` - Offset metadata (if present)
- `{prefix}req-time` - Request timestamp

**Body (JSON):**
```json
{
  "groupId": "my-consumer-group",
  "topicName": "my-topic",
  "partitionIndex": 0,
  "committedOffset": 12345,
  "committedLeaderEpoch": 1,
  "metadata": "optional-metadata"
}
```

### HTTP Response

The HTTP response can modify the offset data by returning specific headers:

| Response Header | Effect |
|-----------------|--------|
| `{prefix}committed-offset` | Override the committed offset value |
| `{prefix}metadata` | Override the offset metadata |
| `{prefix}committed-leader-epoch` | Override the leader epoch |

### Sample Config

```properties
# Basic configuration
enable=true
uri=http://my-service/offset-fetch-hook

# HTTP client
httpClient.class=org.apache.kafka.common.requests.transform.AHC5HttpClient
httpClient.socketTimeout=30
httpClient.connectTimeout=10

# Filtering (optional)
groups.idPattern=^my-app-.*$
topics.namePattern=^(?!__).*$

# Error handling
httpClient.onException=ignore
onException=ignore

# Header prefix
headers.prefix=offset-fetch-broker-
```

### Example: Offset Tracking Service

A simple service that logs offset fetches:

```python
from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/offset-fetch-hook', methods=['POST'])
def offset_fetch():
    data = request.json
    group_id = data.get('groupId', 'unknown')
    topic = data['topicName']
    partition = data['partitionIndex']
    offset = data['committedOffset']

    print(f"Group {group_id} fetched offset {offset} for {topic}-{partition}")

    # Return 200 to allow the offset fetch to proceed unchanged
    # Or return headers to modify the offset:
    # response = make_response('', 200)
    # response.headers['offset-fetch-broker-committed-offset'] = '999'
    # return response

    return '', 200

if __name__ == '__main__':
    app.run(port=8080)
```

---

## HttpOffsetCommitRequestDataTransformer

Intercepts OffsetCommit requests and makes HTTP requests when consumers commit their offsets. This is the key transformer for tracking when records have been processed by consumers.

### Use Cases
- Track consumer progress in real-time
- Correlate produced records with consumer acknowledgment
- Implement exactly-once processing verification
- Build consumer lag monitoring dashboards
- Audit trail for compliance

### Configuration

| Name | Default | Values | Effect |
|------|---------|--------|--------|
| uri | | | URI of the service to forward offset commit data to (required) |
| enable | true | true,false | Is transformation enabled? |
| enable-send | true | true,false | Actually send HTTP requests? |
| groups.idPattern | | regex | Filter by consumer group ID (optional) |
| topics.namePattern | | regex | Filter by topic name (optional) |
| httpClient.class | | class name | HTTP client implementation to use |
| httpClient.onException | fail | fail,pass-thru,ignore | HTTP error handling behavior |
| onException | throw | throw,ignore | Transform exception handling |
| headers.http | | key=value pairs | Additional HTTP headers to include |
| **batch.count** | 1 | integer | Send HTTP after N commits (1 = no batching) |
| **batch.intervalMs** | 0 | milliseconds | Send HTTP after N ms elapsed (0 = disabled) |

### Batching

To reduce HTTP overhead, you can configure batching to only send requests periodically:

```properties
# Send HTTP request every 100 commits OR every 5 seconds (whichever comes first)
batch.count=100
batch.intervalMs=5000
```

**How it works:**
- Tracks state per `groupId:topic:partition`
- Only the **latest** offset is sent (intermediate offsets are skipped)
- HTTP request is sent when either threshold is reached
- Since offset commits are cumulative, skipping intermediate commits loses no information

**Example scenarios:**

| batch.count | batch.intervalMs | Behavior |
|-------------|------------------|----------|
| 1 (default) | 0 (default) | Send every commit (no batching) |
| 100 | 0 | Send every 100th commit |
| 0 | 5000 | Send at most every 5 seconds |
| 100 | 5000 | Send every 100 commits OR 5 seconds |

### HTTP Request Format

**Method:** POST

**Headers:**
- `{prefix}hostname` - Broker hostname
- `{prefix}api-type` - "OffsetCommit"
- `{prefix}group-id` - Consumer group ID
- `{prefix}member-id` - Consumer member ID
- `{prefix}generation-id` - Consumer group generation ID
- `{prefix}group-instance-id` - Static group instance ID (if present)
- `{prefix}topic-name` - Topic name
- `{prefix}partition-index` - Partition index
- `{prefix}committed-offset` - Offset being committed
- `{prefix}committed-leader-epoch` - Leader epoch
- `{prefix}committed-metadata` - Commit metadata (if present)
- `{prefix}req-time` - Request timestamp

**Body (JSON):**
```json
{
  "groupId": "my-consumer-group",
  "memberId": "consumer-1-uuid",
  "generationId": 5,
  "groupInstanceId": "static-consumer-1",
  "topicName": "my-topic",
  "partitionIndex": 0,
  "committedOffset": 12345,
  "committedLeaderEpoch": 1,
  "committedMetadata": "optional-metadata"
}
```

### HTTP Response

The HTTP response can modify the commit data by returning specific headers:

| Response Header | Effect |
|-----------------|--------|
| `{prefix}committed-offset` | Override the offset being committed |
| `{prefix}committed-metadata` | Override the commit metadata |
| `{prefix}committed-leader-epoch` | Override the leader epoch |

### Sample Config

```properties
# Basic configuration
enable=true
uri=http://my-service/offset-commit-hook

# HTTP client
httpClient.class=org.apache.kafka.common.requests.transform.AHC5HttpClient
httpClient.socketTimeout=30
httpClient.connectTimeout=10

# Batching - reduce HTTP overhead
batch.count=100
batch.intervalMs=5000

# Filtering (optional)
groups.idPattern=^my-app-.*$
topics.namePattern=^(?!__).*$

# Error handling
httpClient.onException=ignore
onException=ignore

# Header prefix
headers.prefix=offset-commit-broker-
```

---

## Produce Response Listeners

Act on records after the broker has appended them, with the offsets the broker assigned. Requires the Kafka fork's `ProduceResponseListener` hook, which `KafkaApis` calls from the produce response callback, once the records are appended and the request's `acks` is satisfied.

### How it works

1. While parsing a produce request, after all transformers have run, `TransformingProduceRequestParser` stashes each record's key, headers and timestamp in `ProducedRecords`, per partition. Values are stashed only when `records.body=true`. Stashing has to happen at parse time, because the broker clears the request's records after appending them. It only runs when a listener is configured.
2. When the broker calls the listener, `AbstractProduceResponseListener` pops the stash and gives each record its offset: the partition's `baseOffset` plus the record's position in the batch. Partitions that failed to append are left out. Records are then filtered by `topics.namePattern` and passed to `onAppended`.
3. `onAppended` runs on a broker thread and must not block. To act on records differently, extend `AbstractProduceResponseListener`.

Enable it on the broker, with a system property or env var:
```
-Dorg.apache.kafka.common.requests.ProduceResponseListener=org.apache.kafka.common.requests.transform.HttpProduceResponseListener
KAFKA_PRODUCE_RESPONSE_LISTENER=org.apache.kafka.common.requests.transform.HttpProduceResponseListener
```

Listener settings use the name `produce-response-listener` (change it with `KAFKA_PRODUCE_RESPONSE_LISTENER_NAME`). They resolve like any transformer's: `produce-response-listener.properties`, `-Dproduce-response-listener-{key}`, or env `produce_response_listener_{key}`.

### HttpProduceResponseListener

Queues appended records and POSTs them in batches from a background thread, every `batch.intervalMs`, or sooner once `batch.maxRecords` are queued.

| Name | Default | Effect |
|------|---------|--------|
| uri | | Service URI; the listener is disabled without it |
| enable | true | Turn posting on or off |
| records.headers | | Comma-separated record headers to post, matched case-insensitively |
| records.mode | all | `all` posts every record. `last-per-partition` posts only each partition's latest record per flush, see below |
| records.requireHeaders | false | Only post records that have at least one of `records.headers` |
| records.body | false | Also post each record's value. Values are then kept in memory from parse until the append completes |
| records.bodyEncoding | utf8 | `utf8` or `base64` |
| topics.namePattern | | Only post records of matching topics |
| batch.intervalMs | 1000 | How often queued records are sent |
| batch.maxRecords | 5000 | Max records per HTTP call; reaching it triggers an early send |
| queue.maxRecords | 100000 | Max queued records; beyond this, new ones are dropped with a warning |
| headers.http | | Extra `key=value` HTTP headers |
| headers.prefix | `produce-response-listener-broker-` | Prefix of the request headers below |
| httpClient.* | | Shared HTTP client, see [AHC5 HTTP Client Configuration](#ahc5-http-client-configuration) |

**Method:** POST

**Headers:** `Content-Type: application/json`, `{prefix}hostname`, `{prefix}api-type` = `ProduceResponse`, `{prefix}record-count`

**Body:**
```json
{
  "hostname": "broker-1",
  "partitions": [
    {"topic": "article.in.djml", "partition": 0, "records": [
      {"offset": 1000, "timestamp": 1791302400000, "headers": {"content-lake-api-rec-uid": "content-lake:article.in.djml:2026100617_GREEN_01a11242-..."}},
      {"offset": 1001, "timestamp": 1791302400005, "headers": {}}
    ]}
  ]
}
```

Only the configured headers that a record has are included, so `headers` can be empty unless `records.requireHeaders=true`. `body` is added when `records.body=true`.

Any response other than 200 is logged with the full request and response, and the batch is dropped.

#### records.mode=last-per-partition

Instead of every record, posts only the record with the highest offset in each partition since the last flush, i.e. at most one record per partition every `batch.intervalMs`, whatever the throughput. `records.requireHeaders` is applied first, so it's the latest record that has the headers. `queue.maxRecords` doesn't apply, since at most one record per partition is held.

Use it when the service only needs reference points, e.g. to place a consumer group: map a committed offset to the nearest posted offset at or below it, and resume after that record. A group rehydrated that way re-reads at most about one interval of records, the usual at-least-once behaviour.

#### Example: link content lake uids to offsets

```properties
uri=http://streamproc-content-lake-gateway:3000/streamproc-content-lake-gateway/offsets
records.headers=content-lake-api-rec-uid
records.requireHeaders=true
records.body=false
topics.namePattern=(?i)^(?!__).*$
```

### Caveats

- Records still queued when a broker stops are lost.
- If an idempotent producer retries a batch that was already written, the broker reports the original offsets for the retry. A service keyed by offset should keep the first record it receives for an offset.
- "Appended" means written to the log and replicated as `acks` requires, not fsynced. Kafka doesn't fsync each write.

---

## Correlating Produced Records with Consumer Progress

By combining `HttpProduceRequestDataTransformer` and `HttpOffsetCommitRequestDataTransformer`, you can track the full lifecycle of records:

### Data Flow

```
Producer                    Broker                      Consumer
   |                          |                            |
   |---Produce(offset=100)--->|                            |
   |    [HTTP: record produced]                            |
   |                          |                            |
   |                          |<---Fetch(offset=100)-------|
   |                          |                            |
   |                          |<---OffsetCommit(101)-------|
   |                              [HTTP: offset committed] |
```

### Correlation Keys

Use these fields to correlate events:

| Field | Produce Request | Offset Commit |
|-------|-----------------|---------------|
| Topic | `{prefix}topic-name` | `{prefix}topic-name` |
| Partition | `{prefix}partition-index` | `{prefix}partition-index` |
| Offset | `{prefix}record-offset` | `{prefix}committed-offset` |
| Consumer Group | N/A | `{prefix}group-id` |

### Example: Full Correlation Service

```python
from flask import Flask, request
from datetime import datetime
import redis

app = Flask(__name__)
r = redis.Redis()

@app.route('/produce-hook', methods=['POST'])
def on_produce():
    """Called when a record is produced"""
    topic = request.headers.get('myprefix-topic-name')
    partition = request.headers.get('myprefix-partition-index')
    offset = request.headers.get('myprefix-record-offset')

    # Store production timestamp
    key = f"produced:{topic}:{partition}:{offset}"
    r.set(key, datetime.now().isoformat(), ex=86400)

    print(f"Record produced: {topic}-{partition}@{offset}")
    return '', 200

@app.route('/commit-hook', methods=['POST'])
def on_commit():
    """Called when a consumer commits an offset"""
    data = request.json
    group_id = data['groupId']
    topic = data['topicName']
    partition = data['partitionIndex']
    committed_offset = data['committedOffset']

    # Check all offsets up to committed offset
    for offset in range(committed_offset):
        key = f"produced:{topic}:{partition}:{offset}"
        produced_at = r.get(key)
        if produced_at:
            produced_at = produced_at.decode()
            lag = (datetime.now() - datetime.fromisoformat(produced_at)).total_seconds()
            print(f"Record {topic}-{partition}@{offset} consumed by {group_id} after {lag:.2f}s")
            r.delete(key)

    return '', 200

if __name__ == '__main__':
    app.run(port=8080)
```

