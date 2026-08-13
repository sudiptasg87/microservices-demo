package com.ssg.ai.tweet.runner;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ssg.ai.config.KafkaConfigData;
import com.ssg.ai.kafka.avro.model.TwitterAvroModel;
import com.ssg.ai.kafka.producer.service.KafkaProducer;
import com.ssg.ai.tweet.service.AIService;
import com.ssg.ai.tweet.transformer.TwitterStatusToAvroTransformer;

import lombok.extern.slf4j.Slf4j;

/**
 * 
 */
@Slf4j
@Component
public class AIStreamRunner implements Runnable {

	@Autowired
	private AIService aiService;

	@Autowired
	private KafkaConfigData kafkaConfigData;

	@Autowired
	private KafkaProducer<Long, TwitterAvroModel> kafkaProducer;

	@Autowired
	private TwitterStatusToAvroTransformer twitterStatusToAvroTransformer;

	/**
	 *
	 */
	@Override
	public void run() {
		try {
			String generatedTweet = aiService.generateTweet();
			log.info("Generated Tweet: {}", generatedTweet);
			log.info("Convert generated tweet JSON to TwitterAvroModel");
			TwitterAvroModel twitterAvroModel = twitterStatusToAvroTransformer
					.getTwitterAvroModelFromGeneratedTweet(generatedTweet);
			log.info("Send to kafka");
			kafkaProducer.send(kafkaConfigData.getTopicName(), twitterAvroModel.getUserId(), twitterAvroModel);
			log.info("Successfully sent generated tweet to Kafka topic: {}", kafkaConfigData.getTopicName());
		} catch (Exception ex) {
			log.error("Error processing generated tweet", ex);
		}
	}

}
