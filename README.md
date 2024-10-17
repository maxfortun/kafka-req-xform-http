# kafka-req-xform-http


### Build
Must set `KAFKA_VERSION` for build to work.
```
KAFKA_VERSION=4.0.0-SNAPSHOT ./gradlew jar
```

### Request Headers
All headers have a prefix of `plugin prefx`-`broker`-.  

|Name|Default|Values|Effect|
|---|---|---|---|
|uri|||Uri of the service to forward requests to. Overrides default.|
|enable|true|true,false|Is forwarding to service enabled?|
|in-headers||env,time,timespan,hostname|Include in response headers|
|onHttpException|fail|fail,pass-thru,original|Exception handling behavior|

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

topicNamePattern=(?i)^(?!__).*$
topicNamePattern.scopes=(?i)^(app)$

onHttpException=fail
onHttpException.values=(?i)^(fail|pass-thru|original)$
onHttpException.scopes=(?i)^(app|request)$

#httpHeaderPrefix=content-lake-
httpHeaderPrefix.scopes=(?i)^(app)$

in-headers=(?i)^(env|time|timespan|hostname)$
in-headers.scopes=(?i)^(app|request)$
```
