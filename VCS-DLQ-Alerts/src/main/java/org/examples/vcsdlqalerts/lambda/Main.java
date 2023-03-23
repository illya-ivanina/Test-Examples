package org.examples.vcsdlqalerts.lambda;

/**
 * For testing.
 * @see KafkaConsumerTestDemo
 */
public class Main {
    public static void main(String[] args) {
        var kafkaConsumerTestDemo = new KafkaConsumerTestDemo();
        kafkaConsumerTestDemo.consume(Config.getProperty("KAFKA_TOPIC"));
    }

}