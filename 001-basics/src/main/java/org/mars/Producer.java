package org.mars;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {
    private static final Logger log = LoggerFactory.getLogger(Producer.class);

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {

            for (int j = 0; j < 3; j++) {
                for (int i = 1; i < 100000; i++) {
                    String topic = "PARTITION_TEST";
                    String key = "key_" + j;
                    String value = "key_" + j + " | MessageNumber" + i;

                    ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

                    producer.send(record, (recordMetadata, e) -> {
                        if (recordMetadata == null) {
                            log.warn(e.getMessage());
                        } else {
                            log.info("\nTopico: {}\nPartição: {}\nKey: {}\nOffsets: {}\n", recordMetadata.topic(), recordMetadata.partition(), key,recordMetadata.offset());
                        }
                    });

//                    Thread.sleep(50);
                }
            }

            producer.flush();

        } catch (Exception e) {
            log.info(e.getMessage());
        }


    }
}