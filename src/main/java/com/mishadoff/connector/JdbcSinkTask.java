package com.mishadoff.connector;

import com.mishadoff.connector.jdbc.IJdbcConverter;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

public class JdbcSinkTask extends SinkTask {

    private Map<String, String> props;
    private static final Logger logger = LoggerFactory.getLogger(JdbcSinkTask.class);

    // TODO move to connector
    private IJdbcConverter converter;
    private BasicDataSource pool;

    @Override
    public void start(Map<String, String> props) {
        this.props = props;

        try {
            converter = (IJdbcConverter) Class.forName(props.get("jdbc.converter.class")).newInstance();
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new IllegalArgumentException("Can not create object with defined jdbc.converter.class");
        }

        // setup connection pool (do this once per connector)
        pool = setupConnectionPool();
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        for (SinkRecord record : collection) {
            try (
                    Connection conn = pool.getConnection();
                    PreparedStatement ps = converter.convert(props, conn, record);
            ) {
                int updateCount = ps.executeUpdate();
                logger.info("Succesfully inserted {} items", updateCount);
            } catch (SQLException e) {
                logger.error("SQL Error occured" + e.getMessage());
                throw new IllegalArgumentException(e);
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

    private BasicDataSource setupConnectionPool() {
        // TODO make it fully configurable from external props
        BasicDataSource pool = new BasicDataSource();
        pool.setDriverClassName(props.get("driver.class"));
        pool.setUrl(props.get("jdbc.url"));
        pool.setUsername(props.get("jdbc.username"));
        pool.setPassword(props.get("jdbc.password"));
        //ds.setValidationQuery("select 1");
        pool.setTestOnBorrow(true);
        pool.setAccessToUnderlyingConnectionAllowed(true);
        pool.setMinIdle(0);
        pool.setMaxIdle(5);
        pool.setMaxTotal(100);
        pool.setTimeBetweenEvictionRunsMillis(1000);
        pool.setMinEvictableIdleTimeMillis(5000);
        pool.setMaxWaitMillis(20000);
        return pool;
    }
}
