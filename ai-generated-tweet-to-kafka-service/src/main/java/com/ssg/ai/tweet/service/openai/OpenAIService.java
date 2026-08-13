package com.ssg.ai.tweet.service.openai;

import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.client.ChatClient.CallResponseSpec;
import org.springframework.ai.chat.client.ChatClient.ChatClientRequestSpec;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ssg.ai.config.AIGeneratedTweetToKafkaServiceConfigData;
import com.ssg.ai.tweet.exception.AIGeneratedTweetToKafkaServiceException;
import com.ssg.ai.tweet.service.AIService;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class OpenAIService implements AIService {

	@Autowired
	private AIGeneratedTweetToKafkaServiceConfigData config;

	@Autowired
	private ChatClient chatClient;

	/**
	 *
	 */
	@Override
	public String generateTweet() throws AIGeneratedTweetToKafkaServiceException {
		log.info("Generating tweet using OpenAIService");

		log.info("Starting the chat method with input : {}", config.getPrompt());

		ChatClientRequestSpec requestSpec = chatClient.prompt().user(config.getPrompt()
				.replace(config.getKeywordsPlaceholder(), String.join(",", config.getStreamingDataKeywords())));
		
		log.info("RequestSpec : {}", requestSpec);

		CallResponseSpec responseSpec = requestSpec.call();
		log.info("ResponseSpec : {}", responseSpec);

		return responseSpec.content();
	}

}
