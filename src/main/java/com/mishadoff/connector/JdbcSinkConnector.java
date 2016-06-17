package com.mishadoff.connector;

import com.mishadoff.connector.jdbc.IJdbcConverter;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcSinkConnector extends SinkConnector {

    // TODO remove hardcoded property
    public static final String VERSION = "1.0.0";

    public static final String INTERNAL_JDBC_CONVERTER = "_internal.jdbc.converter";
    public static final String INTERNAL_POOL = "_internal.pool";

    private static final Logger logger = LoggerFactory.getLogger(JdbcSinkConnector.class);

    // connector specific properties
    private Map<String, String> props;

    @Override
    public String version() {
        return VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        this.props = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return JdbcSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        // just pass generic connector properties to each task
        List<Map<String, String>> taskConfigs = new ArrayList<Map<String, String>>();
        Map<String, String> taskProps = new HashMap<String, String>();
        taskProps.putAll(props);
        for (int i = 0; i < maxTasks; i++) {
            taskConfigs.add(taskProps);
        }
        return taskConfigs;
    }

    @Override
    public void stop() {
        // JUST STOP, DO NOTHING
    }

    @Override
    public ConfigDef config() {
        // TODO add more configs
        return new ConfigDef()
            .define("jdbc.url",
                    ConfigDef.Type.STRING,
                    null, // no default value
                    ConfigDef.Importance.HIGH,
                    "JDBC URL to database (jdbc:postgresql://localhost:5432/db)")
            .define("put.mode",
                    ConfigDef.Type.STRING,
                    "insert",
                    ConfigDef.Importance.MEDIUM,
                    "one of following modes: [insert, update]");
    }
}
