
import {SchemaRegistry} from "@kafkajs/confluent-schema-registry";
import axios from "axios";

const SCHEMA_REGISTRY_HOST = process.env.SCHEMA_REGISTRY_HOST;
const SCHEMA_REGISTRY_USER = process.env.SCHEMA_REGISTRY_USER;
const SCHEMA_REGISTRY_PASSWORD = process.env.SCHEMA_REGISTRY_PASSWORD;

const SLACK_WEBHOOK_URL = process.env.SLACK_WEBHOOK_URL;
const SLACK_MESSAGE =
    "A transaction has been found in the VCS Trans Retry DLQ. See attachment for copy. Steps to retry can be found here: " +
    "<https://amwaycloud.atlassian.net/wiki/spaces/IBOE2E/pages/46798815/Conduktor+DLQ+Process+Checkup|https://amwaycloud.atlassian.net/wiki/spaces/IBOE2E/pages/46798815/Conduktor+DLQ+Process+Checkup>";

console.log({"SCHEMA_REGISTRY_HOST": SCHEMA_REGISTRY_HOST});
console.log({"SLACK_WEBHOOK_URL": SLACK_WEBHOOK_URL});

const registry = new SchemaRegistry({
    host: SCHEMA_REGISTRY_HOST,
    auth: {username: SCHEMA_REGISTRY_USER, password: SCHEMA_REGISTRY_PASSWORD}
});
const sendSlackNotification = async (message) => {
    const data = {text: message};
    try {
        const response = await axios.post(SLACK_WEBHOOK_URL, data);
        console.log({">>> sendSlack response >>>":response.data});
    } catch (error) {
        console.log({">>> sendSlack error >>>":error});
    }
}

export const handler = async (event) => {
    console.log({">>> handler event >>>": event});
    for (const key in event.records) {
        console.log({">>> key >>>":key});
        event.records[key].forEach((record) => {
            const value = record.value;
            //const value = await registry.decode(record.value);
            console.log(value);
            const message = `${SLACK_MESSAGE} \n MESSAGE: \n ${value}`;
            sendSlackNotification(message);
        });
    }
};
