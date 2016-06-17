# kafka-connect-jdbc-sink

Kafka connector for loading data from kafka topics to jdbc sources.

Prerequisites:
- Java 1.8+
- Kafka 0.10.0.0
- JDBC Driver to preferred database (Kafka-connect ships with PostgreSQL, MariaDB and SQLite drivers)

# Properties

```
name=jdbc-sink-connector
connector.class=com.mishadoff.connector.JdbcSinkConnector
driver.class=org.postgresql.Driver
tasks.max=1
topics=test-topic
jdbc.url=jdbc:postgres://localhost:5432/db
jdbc.username=root
jdbc.password=root
put.mode=insert
jdbc.table=test_table
```

# Modes

We support following modes

### *INSERT*

```
put.mode=insert
```

This mode generates plain insert query to database table

# How to run?

- Build project, to create jar with dependencies
 
```
mvn clean package
```

- Include jar into classpath before running your kafka-connect

```
CLASSPATH=/path/to/jar-with-deps.jar connect-standalone worker.properties connector.properties
```

*Note:* You also need to include your db driver to classpath (if it is not PostgreSQL, MariaDB, MySQL or SQLite) 

# Worker.properties

- Example [here](src/main/resources/worker.properties)
- Full list of worker properties could be found [here](http://kafka.apache.org/documentation.html#connectconfigs)

# Limitations

- Process only value (not key)
- No batch mode
- No table schema validation
- No connection pool