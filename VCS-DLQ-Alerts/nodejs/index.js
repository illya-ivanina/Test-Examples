
const { SchemaRegistry } = require("@kafkajs/confluent-schema-registry");
const sendSlackNotification = require('./sendSlackNotification');

const SCHEMA_REGISTRY_HOST = process.env.SCHEMA_REGISTRY_HOST;
const SCHEMA_REGISTRY_USER = process.env.SCHEMA_REGISTRY_USER;
const SCHEMA_REGISTRY_PASSWORD = process.env.SCHEMA_REGISTRY_PASSWORD;
const CLIENT_ID = process.env.CLIENT_ID;
const SLACK_WEBHOOK_URL = process.env.SLACK_WEBHOOK_URL;
const SLACK_MESSAGE =
    "A transaction has been found in the VCS Trans Retry DLQ. See attachment for copy. Steps to retry can be found here: " +
    "<https://amwaycloud.atlassian.net/wiki/spaces/IBOE2E/pages/46798815/Conduktor+DLQ+Process+Checkup|https://amwaycloud.atlassian.net/wiki/spaces/IBOE2E/pages/46798815/Conduktor+DLQ+Process+Checkup>";



console.log({"SCHEMA_REGISTRY_HOST": SCHEMA_REGISTRY_HOST});
console.log({"SLACK_WEBHOOK_URL": SLACK_WEBHOOK_URL});

const registry = new SchemaRegistry({
    host: SCHEMA_REGISTRY_HOST,
    auth: {
        username: SCHEMA_REGISTRY_USER,
        password: SCHEMA_REGISTRY_PASSWORD
    },
    clientId: CLIENT_ID
});

const handler = async (event) => {
    console.log({">>> handler event >>>": event});
    for (const key in event.records) {
        console.log({">>> key >>>":key});
        for (const record of event.records[key]) {
            //const value = record.value;
            console.log({">>> Source record >>>": record.value});
            //const base64decodedValue = Buffer.from(record.value, 'base64').toString('utf-8');
            //console.log({">>> Base64 decoded value >>>": base64decodedValue});
            const value = await registry.decode(record.value);
            const message = `${SLACK_MESSAGE} \n MESSAGE: \n ${value}`;
            sendSlackNotification(message).then(r => console.log("Send to Slack: Ok")).catch(error => console.log(error));
        }
    }
};
module.exports.handler = handler;
