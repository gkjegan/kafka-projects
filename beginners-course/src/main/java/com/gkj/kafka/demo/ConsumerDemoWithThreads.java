package com.gkj.kafka.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {

    public static void main(String[] args){
        new ConsumerDemoWithThreads().run();
    }
    private ConsumerDemoWithThreads(){

    }
    private void run(){
        final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class);
        //Create consumer properties
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "java-application";
        String topic = "first_topic";

        //latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        //Create the consumer runnable
        logger.info("Creating the consumer thread");
        Runnable myConsumerRunnable = new CosnumerRunnable(bootstrapServers, groupId, topic, latch);
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook");
            ((CosnumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                logger.info("Application is interrupted.", e);
            } finally {
                logger.info("Application is exited.");
            }
        }
        ));
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.info("Application is interrupted.", e);
        }finally {
            logger.info("Application is closing.");
        }
    }

    public class CosnumerRunnable implements Runnable{

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(CosnumerRunnable.class);
        public CosnumerRunnable(String bootstrapServers, String groupId, String topic, CountDownLatch latch){
            //Create consumer config
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

            this.latch = latch;
            this.consumer = new KafkaConsumer<String, String>(properties);
            this.consumer.subscribe((Arrays.asList(topic)));
        }

        @Override
        public void run() {
            //Poll for new data
            try{
                while (true){
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis((100)));
                    for (ConsumerRecord<String, String> record : records){
                        logger.info("Received new Metadata. \n"+
                                "Key: " + record.key() + "\n" +
                                "Value: " + record.value() + "\n" +
                                "Partition: " + record.partition() + "\n" +
                                "Offset: " + record.offset() + "\n"

                        );
                    }
                }
            } catch (WakeupException e){
                logger.info("Received shutdown signal!");
            }finally {
                consumer.close();
                //tell the main code we are done with consumer.
                latch.countDown();
            }

        }
    public void shutdown() {
        /**
         * The wakeup() method is a special method to interrupt consumer.poll()
         * It will throw the exception WakeUpException
         */
        consumer.wakeup();
    }
    }
}
