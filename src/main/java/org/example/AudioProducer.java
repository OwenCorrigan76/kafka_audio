package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sound.sampled.*;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.Properties;

public class AudioProducer {
    private static final Logger logger = LoggerFactory.getLogger(AudioProducer.class);

    public static void main(String[] args) {
        logger.info("SLF4J is initialized correctly.");

        // Kafka Producer Properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {
            // Load WAV audio file
            File audioFile = new File("input.wav");
            AudioInputStream audioInputStream = AudioSystem.getAudioInputStream(audioFile);
            // Convert WAV to byte array
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            AudioSystem.write(audioInputStream, AudioFileFormat.Type.WAVE, byteArrayOutputStream);
            byte[] audioBytes = byteArrayOutputStream.toByteArray();

            // Send audio bytes to Kafka
            ProducerRecord<String, byte[]> record = new ProducerRecord<>("audio-topic", "audio-key", audioBytes);
            producer.send(record);

            System.out.println("Audio file sent to Kafka successfully!");
            audioInputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
