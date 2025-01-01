package io.owen.audio;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sound.sampled.*;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Scanner;


public class AudioProducer {
    private static final Logger logger = LoggerFactory.getLogger(AudioProducer.class);

    public static void main(String[] args) throws IOException {
        logger.info("SLF4J is initialized correctly.");
        ProducerConfig config = ProducerConfig.configs();
        Properties props = config.getProperties();

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {
            Scanner scanner = new Scanner(System.in);
            System.out.print("Enter the note to play (e.g., A, B, C#): ");
            String note = scanner.nextLine();

            File audioFile = GuitarScale.getAudioFile(note);
            AudioInputStream audioInputStream = AudioSystem.getAudioInputStream(audioFile);
            // Convert WAV to byte array
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            AudioSystem.write(audioInputStream, AudioFileFormat.Type.WAVE, byteArrayOutputStream);
            byte[] audioBytes = byteArrayOutputStream.toByteArray();

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(config.getTopic(), config.getKey(), audioBytes);
            AtomicLong numSent = new AtomicLong(0);
            for (long i = 0; i < config.getMessageCount(); i++) {
                logger.info("Sending messages {}", config.getMessageCount());
                numSent.incrementAndGet();
                Thread.sleep(config.getDelay());
                producer.send(record);
                logger.info("Audio file " + record + " sent to Kafka successfully!");
                audioInputStream.close();
            }
            producer.flush();
            logger.info("All messages flushed. Total messages sent: {}", numSent.get());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
