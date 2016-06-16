package com.mishadoff.connector;

import com.mishadoff.connector.jdbc.JdbcClient;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

public class JdbcSinkTask extends SinkTask {

    private JdbcClient client = new JdbcClient();
    private Map<String, String> props;
    private static final Logger logger = LoggerFactory.getLogger(JdbcSinkTask.class);


    @Override
    public void start(Map<String, String> props) {
        this.props = props;
        // FIXME init connection pool or get from connector?
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        for (SinkRecord record : collection) {
            // TODO batch write?
            try {
                client.write(props, record);
            } catch (SQLException e) {
                logger.error(e.getMessage());
            }
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
        // WE FLUSH DIRECTLY IN put() method
    }

    @Override
    public void stop() {
        // JUST STOP, DO NOTHING
    }

    @Override
    public String version() {
        return JdbcSinkConnector.VERSION;
    }
}
