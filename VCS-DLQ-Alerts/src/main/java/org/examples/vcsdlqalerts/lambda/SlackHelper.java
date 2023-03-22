package org.examples.vcsdlqalerts.lambda;

import com.slack.api.Slack;
import com.slack.api.methods.MethodsClient;
import com.slack.api.methods.SlackApiException;
import com.slack.api.methods.request.chat.ChatPostMessageRequest;
import com.slack.api.model.block.DividerBlock;
import com.slack.api.model.block.LayoutBlock;
import com.slack.api.model.block.SectionBlock;
import com.slack.api.model.block.composition.MarkdownTextObject;
import com.slack.api.webhook.Payload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class SlackHelper {
    private static final Logger log = LoggerFactory.getLogger(SlackHelper.class);
    private final MethodsClient methodsClient;
    private final Slack slack;

    private static final String STATIC_MESSAGE =
            "A transaction has been found in the VCS Trans Retry DLQ. See attachment for copy. Steps to retry can be found here: " +
                    "<https://amwaycloud.atlassian.net/wiki/spaces/IBOE2E/pages/46798815/Conduktor+DLQ+Process+Checkup|https://amwaycloud.atlassian.net/wiki/spaces/IBOE2E/pages/46798815/Conduktor+DLQ+Process+Checkup>";

    public SlackHelper() {
        this.slack = Slack.getInstance();
        String token = Config.getProperty("SLACK_TOKEN");
        this.methodsClient = slack.methods(token);
    }

    public void sendMessage(MessageDTO messageDTO) {

        List<LayoutBlock> message = new ArrayList<>();
        message.add(SectionBlock
                .builder()
                .text(MarkdownTextObject
                        .builder()
                        .text(STATIC_MESSAGE)
                        .build())
                .build());
        message.add(SectionBlock
                .builder()
                .fields(Collections.singletonList(
                        MarkdownTextObject
                                .builder()
                                .text("*Topic:* " + messageDTO.getTopic())
                                .build()
                ))
                .build());
        message.add(SectionBlock
                .builder()
                .fields(Arrays.asList(
                        MarkdownTextObject
                                .builder()
                                .text("*Partition:* " + messageDTO.getPartition())
                                .build(),
                        MarkdownTextObject
                                .builder()
                                .text("*Offset:* " + messageDTO.getOffset())
                                .build()
                ))
                .build());
        message.add(DividerBlock
                .builder()
                .build());
        message.add(SectionBlock
                .builder()
                .fields(Collections.singletonList(
                        MarkdownTextObject
                                .builder()
                                .text("*DATA:* ```" + messageDTO.getMessage() + "```")
                                .build()
                ))
                .build());

        /*
        ChatPostMessageRequest request = ChatPostMessageRequest.builder()
                .channel(Config.getProperty("SLACK_CHANNEL"))
                .blocks(message)
                .build();
        try {
            methodsClient.chatPostMessage(request);
        } catch (IOException | SlackApiException e) {
            throw new RuntimeException(e);
        }*/

        try {
            log.info("Sending message to Slack channel " + Config.getProperty("SLACK_CHANNEL"));
            slack.send(
                    Config.getProperty("SLACK_WEBHOOK_URL"),
                    Payload.builder()
                            .blocks(message).build()
            );
        } catch (IOException e) {
            log.error("******  Failed to send message to Slack channel: " + e.getMessage(), e);
        }
    }

}
