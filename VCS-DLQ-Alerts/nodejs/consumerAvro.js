
const { Kafka } = require('kafkajs');
const { SchemaRegistry } = require('@kafkajs/confluent-schema-registry');
const sendSlackNotification = require('./sendSlackNotification');


const kafka = new Kafka({
    brokers: ['localhost:9092'],
    clientId: 'node-js-test-avro-consumer',
})
const registry = new SchemaRegistry({ host: 'http://localhost:8081/' })
const consumer = kafka.consumer({ groupId: 'test-group' })
const topic = "TEST_DEMO"


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
    })
}

module.exports = consume

