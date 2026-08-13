package com.ssg.ai.kafka.producer.service.impl;

import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import com.ssg.ai.kafka.avro.model.TwitterAvroModel;
import com.ssg.ai.kafka.producer.service.KafkaProducer;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;

/**
 * 
 */
@Slf4j
@Service
public class TwitterKafkaProducer implements KafkaProducer<Long, TwitterAvroModel> {

	@Autowired
	private KafkaTemplate<Long, TwitterAvroModel> kafkaTemplate;

	/**
	 *
	 */
	@Override
	public void send(String topicName, Long key, TwitterAvroModel message) {
		log.info("Sending message='{}' to topic='{}'", message, topicName);
		CompletableFuture<SendResult<Long, TwitterAvroModel>> kafkaResultFuture = kafkaTemplate.send(topicName, key,
				message);
		addCallback(topicName, message, kafkaResultFuture);
	}

	@PreDestroy
	public void close() {
		if (kafkaTemplate != null) {
			log.info("Closing kafka producer!");
			kafkaTemplate.destroy();
		}
	}
	
	/**
	 * @param topicName
	 * @param message
	 * @param kafkaResultFuture
	 */
	private void addCallback(String topicName, TwitterAvroModel message,
			CompletableFuture<SendResult<Long, TwitterAvroModel>> kafkaResultFuture) {
		kafkaResultFuture.whenComplete((result, ex) -> {
			if (ex == null) {
				RecordMetadata metadata = result.getRecordMetadata();
				log.debug("Received new metadata. Topic: {}; Partition {}; Offset {}; Timestamp {}, at time {}",
						metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp(),
						System.nanoTime());
			} else {
				log.error("Error while sending message {} to topic {}", message.toString(), topicName, ex);
			}
		});
	}

}
