const axios = require("axios");

const SLACK_WEBHOOK_URL = process.env.SLACK_WEBHOOK_URL;
const SLACK_MESSAGE =
    "A transaction has been found in the VCS Trans Retry DLQ. See attachment for copy. Steps to retry can be found here: " +
    "<https://amwaycloud.atlassian.net/wiki/spaces/IBOE2E/pages/46798815/Conduktor+DLQ+Process+Checkup|https://amwaycloud.atlassian.net/wiki/spaces/IBOE2E/pages/46798815/Conduktor+DLQ+Process+Checkup>";

/**
 * Use message only in JSON format
 * @param message
 * @returns {Promise<void>}
 */
const sendSlackNotification = async (message) => {
    const data = {
        'text': "VCS DLQ Alert",
        'blocks': [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": SLACK_MESSAGE
                }
            },
            {
                'type': 'section',
                'text': {
                    'text': "```" + JSON.stringify(message) + "```",
                    'type': 'mrkdwn'
                }
            }
        ]};
    try {
        console.log({"sent to slack::":SLACK_WEBHOOK_URL});
        const response = await axios.post(SLACK_WEBHOOK_URL, data);
        console.log({"sendSlack response":response.data});
    } catch (error) {
        console.log({"sendSlack error":error});
    }
}

module.exports = sendSlackNotification