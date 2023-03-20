import {SchemaRegistry} from "@kafkajs/confluent-schema-registry";

const axios = import("axios");

const SCHEMA_REGISTRY_HOST = 'http://localhost:8081/'
const SLACK_WEBHOOK_URL = process.env.SLACK_WEBHOOK_URL;
const SLACK_MESSAGE =
    "A transaction has been found in the VCS Trans Retry DLQ. See attachment for copy. Steps to retry can be found here: <https://amwaycloud.atlassian.net/wiki/spaces/IBOE2E/pages/46798815/Conduktor+DLQ+Process+Checkup|https://amwaycloud.atlassian.net/wiki/spaces/IBOE2E/pages/46798815/Conduktor+DLQ+Process+Checkup>";

const registry = new SchemaRegistry({ host: SCHEMA_REGISTRY_HOST })
const sendSlackNotification = async (message) => {
    const data = { text: message };
    await axios.post(SLACK_WEBHOOK_URL, data);
}
const handler = async(event) => {
    for (const record of event.Records) {
        const key = record.key;
        console.log(key);
        const value = await registry.decode(message.value);
        console.log(value);
        const message = `${SLACK_MESSAGE} \n MESSAGE: \n ${value}`;
        await sendSlackNotification(message);
    }
};
