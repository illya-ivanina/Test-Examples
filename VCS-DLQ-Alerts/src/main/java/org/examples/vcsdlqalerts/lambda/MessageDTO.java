package org.examples.vcsdlqalerts.lambda;

import lombok.Builder;
import lombok.Data;

/**
 * The DTO structure for Slack message.
 * It contains more important information, that should be sent to Slack
 * The field `message` actually should contain string in JSON format,
 * and that will ber passed like block of code
 */
@Builder
@Data
public class MessageDTO {
    private String topic;
    private int partition;
    private long offset;
    private String message;
}
