package com.ssg.ai.tweet.transformer;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ssg.ai.kafka.avro.model.TwitterAvroModel;

import lombok.extern.slf4j.Slf4j;

/**
 * 
 */
@Slf4j
@Component
public class TwitterStatusToAvroTransformer {

	/**
	 * Convert generated AI tweet JSON string to TwitterAvroModel
	 */
	public TwitterAvroModel getTwitterAvroModelFromGeneratedTweet(String generatedTweetJson) {
		try {
			JsonNode jsonNode = new ObjectMapper().readTree(generatedTweetJson);
			
			return TwitterAvroModel
					.newBuilder()
					.setId(jsonNode.get("id").asLong())
					.setUserId(jsonNode.get("user").get("id").asLong())
					.setText(jsonNode.get("text").asText())
					.setCreatedAt(System.currentTimeMillis())
					.build();
		} catch (Exception e) {
			log.error("Error transforming generated tweet: {}", generatedTweetJson, e);
			throw new RuntimeException("Failed to transform generated tweet to Avro model", e);
		}
	}
	
}
