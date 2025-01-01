package io.owen.audio;

import java.io.IOException;
import java.util.Properties;

public class ProducerConfig {
    private static final long DEFAULT_MESSAGES_COUNT = 1;
    private static final String DEFAULT_TOPIC = "myTopic";
    private static final String DEFAULT_KEY = "myKey";

    private final Properties props;
    private final String topic;
    private final String key;
    private final int delay;
    private final Long messageCount;

    public ProducerConfig(Properties properties, String topic, String key, int delay, Long messageCount) {
        this.props = properties;
        this.topic = topic;
        this.key = key;
        this.delay = delay;
        this.messageCount = messageCount;
    }

    public static ProducerConfig configs() throws IOException {
        String topic = System.getenv("DEFAULT_TOPIC") == null ? DEFAULT_TOPIC : (System.getenv("DEFAULT_TOPIC"));
        String key = System.getenv("DEFAULT_KEY") == null ? DEFAULT_KEY : (System.getenv("DEFAULT_KEY"));
        int delay = 1000;
        Long messageCount = System.getenv("MESSAGE_COUNT") == null ? DEFAULT_MESSAGES_COUNT : Long.parseLong(System.getenv("MESSAGE_COUNT"));
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        return new ProducerConfig(props, topic, key, delay, messageCount);
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

    public Long getMessageCount() {
        return messageCount;
    }

    public Properties getProperties(){
        return props;
    }



    @Override
    public String toString() {
        return "ProducerConfig{" +
                "props=" + props +
                ", topic='" + topic + '\'' +
                ", key='" + key + '\'' +
                ", delay=" + delay +
                ", messageCount=" + messageCount +
                '}';
    }
}
