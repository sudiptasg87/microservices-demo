package com.ssg.ai.tweet.init;

import java.util.Arrays;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import com.ssg.ai.tweet.config.AIGeneratedTweetToKafkaServiceConfigData;

import lombok.extern.slf4j.Slf4j;

/**
 * 
 */
@Slf4j
@Component
public class ApplicationInit implements ApplicationRunner {
	
	@Autowired
	private AIGeneratedTweetToKafkaServiceConfigData config;

	@Override
	public void run(ApplicationArguments args) throws Exception {
		log.info("ai-generated-tweet-to-kafka-service started");
		log.info("Keywords: {}",  Arrays.toString(config.getStreamingDataKeywords().toArray(new String[] {})));
	}

}
