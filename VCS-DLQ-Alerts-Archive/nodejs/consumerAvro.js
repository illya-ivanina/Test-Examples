/**
 * ENV:
 * SCHEMA_REGISTRY_HOST=http://localhost:8081
 * SCHEMA_REGISTRY_USER
 * SCHEMA_REGISTRY_PASSWORD
 * KAFKA_BOOTSTRAP_SERVERS=localhost:9092
 * KAFKA_USER
 * KAFKA_PASSWORD
 * KAFKA_TOPIC=TEST_DEMO
 * GROUP_ID
 * CLIENT_ID
 *
 * Windows:
 * @echo off
 * setlocal EnableDelayedExpansion
 * set ...
 * @echo on
 */
const { Kafka } = require('kafkajs');
const { SchemaRegistry } = require('@kafkajs/confluent-schema-registry');
const sendSlackNotification = require('./sendSlackNotification');


const kafka = new Kafka({
    brokers: [process.env.KAFKA_BOOTSTRAP_SERVERS],
    clientId: process.env.CLIENT_ID,
    ssl: true,
    sasl: {
        mechanism: 'plain',
        username: process.env.KAFKA_USER,
        password: process.env.KAFKA_PASSWORD,
    }
})
//const registry = new SchemaRegistry({ host: 'http://localhost:8081/' })
const registry = new SchemaRegistry({
    host: process.env.SCHEMA_REGISTRY_HOST,
    auth: {
        username: process.env.SCHEMA_REGISTRY_USER,
        password: process.env.SCHEMA_REGISTRY_PASSWORD
    },
    clientId: process.env.CLIENT_ID
});
const consumer = kafka.consumer({
    groupId: process.env.GROUP_ID,
    maxPollInterval: 30000
});
const topic = process.env.KAFKA_TOPIC;


const consume = async () => {
    await consumer.connect()
    await consumer.subscribe({ topic: topic }) //, fromBeginning: true

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const key = message.key
            const value = await registry.decode(message.value)
            console.log({ key, value })
            console.log({ "metadata::sourceEvent": value['metadata'] })
            sendSlackNotification(value)
                .then(r => console.log(r))
                .catch(e => console.log(e));
        },
        maxBatchSize: 1
    })
}

module.exports = consume

