package com.ssg.ai.kafka.config.admin;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

/**
 * 
 */
@Configuration
public class WebClientConfig {
	
	/**
	 * @return
	 */
	@Bean
	public WebClient webClient() {
		return WebClient.builder().build();
	}

}
