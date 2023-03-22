package org.examples.vcsdlqalerts.lambda;

import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Util {
    private static final Logger log = LoggerFactory.getLogger(Util.class);

    public static void sendToSlack(String message) {

        OkHttpClient client = new OkHttpClient();
        MediaType mediaType = MediaType.parse("application/json; charset=utf-8");
        RequestBody body = RequestBody.create(mediaType,
                "{\"text\":\"" +
                        message.replaceAll("\"", "\\\\\"") +
                        "\",\"channel\":\"" + Config.getProperty("SLACK_CHANNEL") + "\"}");
        Request request = new Request.Builder()
                .url(Config.getProperty("SLACK_WEBHOOK_URL"))
                .post(body)
                .build();
        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException(
                        String.format("Unexpected response code %d; \nMessage: %s; \n",
                                response.code(), response.message()));
            }
            log.info("Message sent to Slack channel " + Config.getProperty("SLACK_CHANNEL") + ": " + message);
        } catch (IOException e) {
            throw new RuntimeException("Failed to send message to Slack channel: " + e.getMessage(), e);
        }
    }

}
