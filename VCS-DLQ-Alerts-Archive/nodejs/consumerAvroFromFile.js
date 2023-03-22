
const { Kafka } = require('kafkajs')
const avro = require('avsc')
const fs = require('fs')
const path = require('path')


const schemaFileName = 'avroSDLQSchemaMini.avsc'

const kafka = new Kafka({
    brokers: ['localhost:9092'],
    clientId: 'node-js-test-avro-consumer',
    retry: false
})
const consumer = kafka.consumer({ groupId: 'test-group' })
const topic = "TEST_DEMO"

const schema = avro.parse(fs.readFileSync(path.join(__dirname, schemaFileName), 'utf-8'));
console.log({ ">>> schema": schema })


const schemaJson = fs.readFileSync(path.join(__dirname, schemaFileName), 'utf-8');
const avroSchema = JSON.parse(schemaJson);

const type = avro.Type.forSchema(avroSchema);

const consume = async () => {
    await consumer.connect()
    await consumer.subscribe({ topic: topic })

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const key = message.key
            //const value = schema.fromBuffer(message.value)
            //const value = decoder.decode(message.value)
            const value = type.fromBuffer(message.value);
            console.log({ key, value })
        },
    })
}

module.exports = consume