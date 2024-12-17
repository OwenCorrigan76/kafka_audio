package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sound.sampled.*;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.Collections;
import java.util.Properties;

public class AudioConsumer {
    private static final Logger logger = LoggerFactory.getLogger(AudioProducer.class);

    public static void main(String[] args) {
        logger.info("SLF4J is initialized correctly.");

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "audio-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singleton("audio-topic"));
            while (true) {
                // Polling for messages
                ConsumerRecords<String, byte[]> records = consumer.poll(1000);
                for (ConsumerRecord<String, byte[]> record : records) {
                    byte[] audioBytes = record.value();
                    // Save received bytes as a WAV file
                    File outputFile = new File("output.wav");
                    try (FileOutputStream fos = new FileOutputStream(outputFile)) {
                        fos.write(audioBytes);
                    }

                    System.out.println("Audio file saved as output.wav");
                    // Play the audio (optional)
                    try (AudioInputStream audioInputStream = AudioSystem.getAudioInputStream(new ByteArrayInputStream(audioBytes))) {
                        Clip clip = AudioSystem.getClip();
                        clip.open(audioInputStream);
                        clip.start();
                        System.out.println("Playing audio...");
                        Thread.sleep(clip.getMicrosecondLength() / 1000);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
