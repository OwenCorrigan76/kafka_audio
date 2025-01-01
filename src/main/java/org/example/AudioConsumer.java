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
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

public class AudioConsumer {
    private static final Logger logger = LoggerFactory.getLogger(AudioProducer.class);

    public static void main(String[] args) throws IOException {
        logger.info("SLF4J is initialized correctly.");

        ConsumerConfig config = ConsumerConfig.configs();
        Properties props = config.getProperties();

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singleton(config.getTopic()));
            while (true) {
                // Polling for messages
                ConsumerRecords<String, byte[]> records = consumer.poll(config.getDelay());
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
