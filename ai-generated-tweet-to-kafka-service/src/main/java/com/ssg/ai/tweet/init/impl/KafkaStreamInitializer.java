package com.ssg.ai.tweet.init.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ssg.ai.config.KafkaConfigData;
import com.ssg.ai.kafka.config.admin.client.KafkaAdminClient;
import com.ssg.ai.tweet.init.StreamInitializer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class KafkaStreamInitializer implements StreamInitializer {
	
	@Autowired
	private KafkaConfigData kafkaConfigData;
	
	@Autowired
	private KafkaAdminClient kafkaAdminClient;

	/**
	 *
	 */
	@Override
	public boolean init() {
		try {
			kafkaAdminClient.createTopics();
			kafkaAdminClient.checkSchemaRegistry();
			log.info("Topics with name {} is ready for operations!", kafkaConfigData.getTopicNamesToCreate().toArray());
			return true;
		} catch (Exception ex) {
			log.error("Error while creating topics, hence returning false");
			return false;
		}
	}

	
}
