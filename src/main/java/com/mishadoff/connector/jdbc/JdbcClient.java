package com.mishadoff.connector.jdbc;

import com.mishadoff.connector.utils.StringUtils;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JdbcClient {

    private static final Logger logger = LoggerFactory.getLogger(JdbcClient.class);

    public void write(Map<String, String> props, SinkRecord sinkRecord) throws SQLException {
        Connection connection = getConnection();
        // TODO use query dsl to generate database specific query
        StringBuilder insertQuery = new StringBuilder();
        insertQuery.append("insert into ");
        insertQuery.append(props.get("jdbc.table"));
        Schema schema = sinkRecord.valueSchema();
        Object value = sinkRecord.value();
        if (value instanceof Struct) {
            Struct struct = (Struct) value;
            List<Field> fields = schema.fields();
            List<String> fieldNames = fields.stream().map(f -> f.name()).collect(Collectors.toList());
            insertQuery.append(StringUtils.fieldsClause(fieldNames));
            insertQuery.append(" values");
            insertQuery.append(StringUtils.placeholderClause(fieldNames));
            PreparedStatement ps = connection.prepareStatement(insertQuery.toString());

            for (int i = 0; i < fields.size(); i++) {
                Field f = fields.get(i);
                Schema.Type fType = f.schema().type();
                switch (fType) {

                    // integers
                    case INT8:
                    case INT16:
                    case INT32:
                    case INT64: {
                        ps.setInt(i + 1, (Integer) struct.get(f.name()));
                        break;
                    }

                    // double
                    case FLOAT32:
                    case FLOAT64: {
                        ps.setDouble(i + 1, (Double) struct.get(f.name()));
                        break;
                    }

                    case STRING: {
                        ps.setString(i + 1, (String) struct.get(f.name()));
                        break;
                    }

                    default: {
                        ps.setObject(i + 1, null);
                        logger.warn("Invalid type " + fType + " (using null)");
                    }
                }
            }

            logger.info("Query: " + insertQuery.toString());

            // run query
            ps.executeUpdate();

            // TODO close connection and statement

        } else {
            throw new IllegalArgumentException("Only STRUCT messages are supported");
        }
    }

    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:postgresql://localhost:5432/mkoz");
    }
}
