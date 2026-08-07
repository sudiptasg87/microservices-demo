package com.ssg.ai.tweet.service;

import com.ssg.ai.tweet.exception.AIGeneratedTweetToKafkaServiceException;

/**
 * 
 */
public interface AIService {
	
	/**
	 * @return
	 * @throws AIGeneratedTweetToKafkaServiceException
	 */
	String generateTweet() throws AIGeneratedTweetToKafkaServiceException;
	
}
