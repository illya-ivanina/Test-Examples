const path = require('path')
const { Kafka } = require('kafkajs')
const { SchemaRegistry, SchemaType, avdlToAVSCAsync } = require('@kafkajs/confluent-schema-registry')

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
        },
    })
}


/*const consume = async () => {
    const schema = await avdlToAVSCAsync(path.join(__dirname, 'avroSDLQSchema.avsc'))
    const { id } = await registry.register({ type: SchemaType.AVRO, schema: JSON.stringify(schema) })
    await consumer.connect()
    await consumer.subscribe({ topic: topic })
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const decodedMessage = {
                ...message,
                value: await registry.decode(message.value)
            }
            console.log(`received message: ${decodedMessage.value}`)
        },
    })
}*/
module.exports = consume