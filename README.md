# kafka-connect-jdbc-sink

Kafka connector for writing data to jdbc sources.
Table must exist.

Prerequisites:
- Java 1.8+
- Kafka 0.10.0.0

# Properties

```
name=jdbc-sink-connector
connector.class=com.mishadoff.connector.JdbcSinkConnector
tasks.max=1
topics=test-topic
jdbc.url=jdbc:postgres://localhost:5432/db
put.mode=insert
```

# `put.mode`

We support two different modes for adding records into database


- `insert` mode generates insert clause and just adds document to the table
- `update` mode generates update clause wit where for `update.key` in properties 
and set for all other values

# How to run?

1. Build project, to create jar with dependencies
 
```
mvn clean package
```

2. Include it into classpath before running your kafka-connect

```
CLASSPATH=/path/to/jar-with-deps.jar connect-standalone worker.properties connector.properties
```

# Limitations

- No batch mode
- No table schema validation
- No connection pool