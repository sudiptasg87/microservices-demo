package com.ssg.ai.kafka.config.admin;

import java.util.Map;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.annotation.EnableRetry;

import com.ssg.ai.config.KafkaConfigData;

/**
 * 
 */
@EnableRetry
@Configuration
public class KafkaAdminConfig {

	@Autowired
	private KafkaConfigData kafkaConfigData;
	
	/**
	 * @return
	 */
	@Bean
	public AdminClient adminClient() {
		return AdminClient.create(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaConfigData.getBootstrapServers()));
	}
	
}
