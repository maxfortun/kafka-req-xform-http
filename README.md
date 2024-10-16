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
|enabled|true|true,false|Is forwarding to service enabled?|
|in-headers||env,time,timespan|Include in response headers|
|onHttpException|fail|fail,pass-thru,original|Exception handling behavior|
