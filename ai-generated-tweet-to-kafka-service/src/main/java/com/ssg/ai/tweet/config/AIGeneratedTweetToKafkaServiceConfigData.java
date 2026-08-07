package com.ssg.ai.tweet.config;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.Data;

/**
 * 
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "ai-generated-tweet-to-kafka-service")
public class AIGeneratedTweetToKafkaServiceConfigData {
	
	private List<String> streamingDataKeywords;
	private Long schedulerDurationSec;
	private String prompt;
	private String keywordsPlaceholder;

}
