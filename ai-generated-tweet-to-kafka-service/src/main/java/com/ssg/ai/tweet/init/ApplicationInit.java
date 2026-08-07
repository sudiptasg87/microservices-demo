package com.ssg.ai.tweet.init;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.stereotype.Component;

import com.ssg.ai.tweet.config.AIGeneratedTweetToKafkaServiceConfigData;
import com.ssg.ai.tweet.runner.AIStreamRunner;

import lombok.extern.slf4j.Slf4j;

/**
 * 
 */
@Slf4j
@EnableScheduling
@Component
public class ApplicationInit implements ApplicationRunner {

	@Autowired
	private AIGeneratedTweetToKafkaServiceConfigData config;

	@Autowired
	private StreamInitializer streamInitializer;

	@Autowired
	private AIStreamRunner aiStreamRunner;

	@Autowired
	private TaskScheduler taskScheduler;

	@Override
	public void run(ApplicationArguments args) throws Exception {
		log.info("ai-generated-tweet-to-kafka-service started");

		if (streamInitializer.init()) {
			log.info("Starting AI Stream Runner with fixed rate {} seconds!", config.getSchedulerDurationSec());
			taskScheduler.scheduleAtFixedRate(aiStreamRunner,
					Duration.of(config.getSchedulerDurationSec(), ChronoUnit.SECONDS));
		} else {
			log.error("Stream Initializer failed to initialize the streams! Not starting the AI Stream Runner!");
		}

		log.info("Keywords: {}", Arrays.toString(config.getStreamingDataKeywords().toArray(new String[] {})));
	}

}
