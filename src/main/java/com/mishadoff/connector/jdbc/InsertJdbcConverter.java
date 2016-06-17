package com.mishadoff.connector.jdbc;

import com.mishadoff.connector.utils.StringUtils;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class InsertJdbcConverter implements IJdbcConverter {

    private static final Logger logger = LoggerFactory.getLogger(InsertJdbcConverter.class);

    @Override
    public PreparedStatement convert(
            Map<String, String> props,
            Connection connection,
            SinkRecord record) throws SQLException {
        // process only value
        Schema recordSchema = record.valueSchema();
        Object recordValueO  = record.value();

        // process only struct value
        if (recordSchema.type() == Schema.Type.STRUCT) {
            Struct recordValue = (Struct) recordValueO;

            // recordName reflects database table we need to write to
            String recordName = recordSchema.name();
            List<Field> recordFields = recordSchema.fields();

            StringBuilder insertQuery = new StringBuilder();
            insertQuery.append("insert into ");

            String tableName = props.getOrDefault("jdbc.target.table.prefix", "") + recordName;
            insertQuery.append(tableName);

            List<String> fieldNames = recordFields.stream().map(f -> f.name()).collect(Collectors.toList());
            insertQuery.append(StringUtils.fieldsClause(fieldNames));
            insertQuery.append(" values");
            insertQuery.append(StringUtils.placeholderClause(fieldNames));
            PreparedStatement ps = connection.prepareStatement(insertQuery.toString());

            for (int i = 0; i < recordFields.size(); i++) {
                Field f = recordFields.get(i);
                Schema.Type fType = f.schema().type();
                switch (fType) {

                    // integers
                    case INT8:
                    case INT16:
                    case INT32:
                    case INT64: {
                        ps.setInt(i + 1, (Integer) recordValue.get(f.name()));
                        break;
                    }

                    // double
                    case FLOAT32:
                    case FLOAT64: {
                        ps.setDouble(i + 1, (Double) recordValue.get(f.name()));
                        break;
                    }

                    case STRING: {
                        ps.setString(i + 1, (String) recordValue.get(f.name()));
                        break;
                    }

                    case BYTES: {
                        String logicalSchemaName = f.schema().name();

                        switch (logicalSchemaName) {
                            case Decimal.LOGICAL_NAME : {
                                BigDecimal value = (BigDecimal) recordValue.get(f.name());
                                ps.setBigDecimal(i + 1, value);
                                break;
                            }
                            default: {
                                ps.setObject(i + 1, null);
                                logger.warn("Unknow logical schema " + logicalSchemaName + " (using null)");
                            }
                        }
                        break;
                    }

                    default: {
                        ps.setObject(i + 1, null);
                        // TODO better resolution?
                        logger.warn("Invalid type " + fType + " (using null)");
                    }
                }
            }

            logger.info("Query: " + insertQuery.toString());

            return ps;
        } else {
            throw new IllegalArgumentException("Only STRUCT for the top-level message are supported");
        }
    }
}
