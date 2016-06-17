package com.mishadoff.connector.jdbc;

import org.apache.kafka.connect.sink.SinkRecord;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

public interface IJdbcConverter {

    /**
     * Having database connection and kafka-connect sink record
     * you need to convert it to prepared statement, which later
     * will be executed by connector.</br>
     *
     * Note: it could be insert query for loading raw data, as well as
     * update query for modifying existing data
     *
     * (!) DO NOT EXECUTE STATEMENT
     *
     * @param props      - connector properties, just if you need it
     * @param connection - valid database connection
     * @param record     - record structure, comes from kafka-connect
     * @return prepared statement with all arguments set to valid values
     * @throws SQLException if we wrong operation executed on prepared statement
     */
    PreparedStatement convert(
            Map<String, String> props,
            Connection connection,
            SinkRecord record) throws SQLException;
}
