package org.examples.vcsdlqalerts.lambda;

import lombok.Builder;

@Builder
public class MessageDTO {
    private String topic;
    private int partition;
    private long offset;
    private String message;
}
