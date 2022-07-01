package com.gkj.kafka.demo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
    //Create Producer Properties
        String bootstrapServers = "127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    //Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

    //Send data
        for (int i =0; i< 10; i++){

            String topic = "first_topic";
            String value = "Hello World " + Integer.toString(i);
            String key = "id_"+ Integer.toString(i);
            /**
             * id 0 - p1
             * id 1 - p0
             * id 2 - p2
             * id 3 - p0
             * id 4 - p2
             * id 5 - p2
             * id 6 = p0
             * id 7 - p2
             * id 8 - p1
             * id 9 - p2
             */
            //Create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,key, value);
            logger.info("Key: "+ key);
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // execute every time a record is successfully sent or an exception is thrown
                    if (e == null){
                        logger.info("Received new Metadata. \n"+
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp() + "\n"
                        );
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            }).get(); // block the .send() to make it synchronous - bad idea for production code.
        }

    //Flush and close data
        producer.flush();
        producer.close();
    }
}
