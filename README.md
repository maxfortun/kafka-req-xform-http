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
|headers.in||env,time,timespan,hostname|Include in response headers|
|httpClient.onException|fail|fail,pass-thru,original|Exception handling behavior|

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

#httpHeaderPrefix=content-lake-
httpHeaderPrefix.scopes=(?i)^(app)$

in-headers=(?i)^(env|time|timespan|hostname)$
in-headers.scopes=(?i)^(app|request)$
```
