package com.gkj.kafka.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * Assign and Seek are mostly used to replay data or fetch a sepcific message
 */
public class ConsumerDemoAssignSeek {
    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);
    //Create consumer properties
        String bootstrapServers = "127.0.0.1:9092";
    //Removing group_id
    //String groupId = "java-application";
        String topic = "first_topic";
    //Create consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    //Removing group_id config
    //properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

    //Create the consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
    //Will not subscribe consumer to topics
    //consumer.subscribe((Arrays.asList(topic)));

    //assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        consumer.assign(Arrays.asList(partitionToReadFrom));
    //seek
        long offsetToReadFrom = 15L;
        consumer.seek(partitionToReadFrom, offsetToReadFrom);
        int numOfMessageToRead = 5;
        boolean keepOnReading = true;
        int currentMessage = 0;

    //Poll for new data
        while (keepOnReading){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis((100)));
            for (ConsumerRecord<String, String> record : records){
                currentMessage++;
                logger.info("Received new Metadata. "+
                        "Key: " + record.key() + " " +
                        "Value: " + record.value() + " " +
                        "Partition: " + record.partition() + " " +
                        "Offset: " + record.offset() + " "

                );
                if (currentMessage > numOfMessageToRead){
                    keepOnReading = false;
                    break;
                }
            }
        }

    }
}
