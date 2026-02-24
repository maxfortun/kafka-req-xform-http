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
All headers have a prefix of `plugin prefix`-`broker`-.

|Name|Default|Values|Effect|
|---|---|---|---|
|uri|||Uri of the service to forward requests to. Overrides default.|
|enable|true|true,false|Is forwarding to service enabled?|
|headers.in||env,time,timespan,hostname|Include in response headers|
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
| `kafka.KEY` | Record key (if present) |

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

---

## AHC5 HTTP Client Configuration

The `AHC5HttpClient` (Apache HttpClient 5) provides configurable connection pooling and timeout settings. All parameters use the `httpClient.` prefix.

### Connection Parameters

| Name | Default | Type | Description |
|------|---------|------|-------------|
| `httpClient.soTimeout` | Infinite | seconds | Socket SO_TIMEOUT - max time waiting for data on an established connection |
| `httpClient.socketTimeout` | Infinite | seconds | Socket timeout for connection-level operations |
| `httpClient.connectTimeout` | Infinite | seconds | Timeout for establishing a new connection |
| `httpClient.connTimeToLiveInMinutes` | 10 | minutes | Maximum time a connection can be kept alive in the pool |
| `httpClient.maxConnPerRoute` | 200 | integer | Maximum connections per route (host) |
| `httpClient.maxConnTotal` | 1000 | integer | Maximum total connections in the pool |

### Connection Pool Settings

The AHC5 client uses a pooling connection manager with:
- **TCP_NODELAY**: Enabled (reduces latency)
- **SO_KEEPALIVE**: Enabled (detects dead connections)
- **Pool Concurrency Policy**: LAX (allows concurrent access)
- **Pool Reuse Policy**: LIFO (reuses most recently used connections)

### Sample AHC5 Configuration

```properties
# HTTP client class
httpClient.class=org.apache.kafka.common.requests.transform.AHC5HttpClient

# Timeouts (in seconds)
httpClient.soTimeout=60
httpClient.socketTimeout=30
httpClient.connectTimeout=10

# Connection pool
httpClient.connTimeToLiveInMinutes=5
httpClient.maxConnPerRoute=100
httpClient.maxConnTotal=500

# Error handling
httpClient.onException=fail
```

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

