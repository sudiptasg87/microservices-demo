package com.ssg.ai.kafka.config.admin.exception;

/**
 * 
 */
public class KafkaClientException extends RuntimeException {

	public KafkaClientException() {
	}

	public KafkaClientException(String message) {
		super(message);
	}

	public KafkaClientException(String message, Throwable cause) {
		super(message, cause);
	}

}
