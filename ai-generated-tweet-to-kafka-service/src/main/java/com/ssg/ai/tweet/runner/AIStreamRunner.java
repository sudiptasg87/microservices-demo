package com.ssg.ai.tweet.runner;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ssg.ai.tweet.service.AIService;

import lombok.extern.slf4j.Slf4j;

/**
 * 
 */
@Slf4j
@Component
public class AIStreamRunner implements Runnable {

	@Autowired
	private AIService aiService;
	
	@Override
	public void run() {
		String generatedTweet = aiService.generateTweet();
		log.info("Generated Tweet: {}", generatedTweet);
	}

}
