package com.ssg.ai.tweet.config;

import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.client.advisor.SimpleLoggerAdvisor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import lombok.extern.slf4j.Slf4j;

/**
 * 
 */
@Slf4j
@Configuration
public class SpringAIConfig {
	
	/**
	 * @param chatClientBuilder
	 * @return
	 */
	@Bean
	public ChatClient chatClient(ChatClient.Builder chatClientBuilder) {
		log.info("Initializing ChatClient without default advisors");
		ChatClient client = chatClientBuilder.defaultAdvisors(new SimpleLoggerAdvisor()).build();
		log.info("ChatClient initialized successfully: {}", client.getClass().getName());
		return client;
	}

}
