package org.example;

import java.io.IOException;
import java.util.Properties;

public class ConsumerConfig {
    private static final String DEFAULT_TOPIC = "myTopic";
    private static final String DEFAULT_KEY = "myKey";
    //private static final String DEFAULT_GROUP_ID = "myGroup";

    private final Properties props;
    private final String topic;
    private final String key;
    private final int delay;
    //private final String groupId;

    public ConsumerConfig(Properties properties, String topic, String key, int delay /*String groupId*/) {
        this.props = properties;
        this.topic = topic;
        this.key = key;
        this.delay = delay;
        //this.groupId = groupId;
    }

    public static ConsumerConfig configs() throws IOException {
        String topic = System.getenv("DEFAULT_TOPIC") == null ? DEFAULT_TOPIC : (System.getenv("DEFAULT_TOPIC"));
        String key = System.getenv("DEFAULT_KEY") == null ? DEFAULT_KEY : (System.getenv("DEFAULT_KEY"));
        int delay = 1000;
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "myGroup");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        return new ConsumerConfig(props, topic, key, delay);
    }

    public int getDelay() {
        return delay;
    }

    public String getKey() {
        return key;
    }

    public String getTopic() {
        return topic;
    }

    public Properties getProperties(){
        return props;
    }

    @Override
    public String toString() {
        return "ConsumerConfig{" +
                "props=" + props +
                ", topic='" + topic + '\'' +
                ", key='" + key + '\'' +
                ", delay=" + delay +
              //  ", groupId='" + groupId + '\'' +
                '}';
    }
}
