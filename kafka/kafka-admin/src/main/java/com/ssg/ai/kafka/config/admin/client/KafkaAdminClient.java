package com.ssg.ai.kafka.config.admin.client;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import com.ssg.ai.config.KafkaConfigData;
import com.ssg.ai.config.RetryConfigData;
import com.ssg.ai.kafka.config.admin.exception.KafkaClientException;

import lombok.extern.slf4j.Slf4j;
import reactor.core.scheduler.Schedulers;

/**
 * 
 */
@Slf4j
@Component
public class KafkaAdminClient {

	@Autowired
	private KafkaConfigData kafkaConfigData;

	@Autowired
	private RetryConfigData retryConfigData;

	@Autowired
	private AdminClient adminClient;

	@Autowired
	private RetryTemplate retryTemplate;

	@Autowired
	private WebClient webClient;

	/**
	 * 
	 */
	public void createTopics() {
		CreateTopicsResult createTopicsResult = null;
		try {
			createTopicsResult = retryTemplate.execute(this::doCreateTopics);
			log.info("Create topic result {}", createTopicsResult.values().values());
		} catch (Throwable t) {
			throw new KafkaClientException("Reached max number of retry for creating kafka topic(s)!", t);
		}
		checkTopicsCreated();
	}

	/**
	 * 
	 */
	public void checkTopicsCreated() {
		Collection<TopicListing> topics = getTopics();
		int retryCount = 1;
		Integer maxAttempts = retryConfigData.getMaxAttempts();
		int multiplier = retryConfigData.getMultiplier().intValue();
		Long sleepTimeMs = retryConfigData.getSleepTimeMs();

		for (String topic : kafkaConfigData.getTopicNamesToCreate()) {
			while (!isTopicCreated(topics, topic)) {
				checkMaxRetry(retryCount++, maxAttempts);
				sleep(sleepTimeMs);
				sleepTimeMs *= multiplier;
				topics = getTopics();
			}
		}
	}

	public void checkSchemaRegistry() {
		int retryCount = 1;
		Integer maxAttempts = retryConfigData.getMaxAttempts();
		int multiplier = retryConfigData.getMultiplier().intValue();
		Long sleepTimeMs = retryConfigData.getSleepTimeMs();

		while (!getSchemaRegistryStatus().is2xxSuccessful()) {
			checkMaxRetry(retryCount++, maxAttempts);
			sleep(sleepTimeMs);
			sleepTimeMs *= multiplier;
		}

	}

	/**
	 * @return
	 */
	private HttpStatus getSchemaRegistryStatus() {
		try {
			return webClient.get().uri(kafkaConfigData.getSchemaRegistryUrl()).retrieve().toBodilessEntity()
					.map(entity -> HttpStatus.valueOf(entity.getStatusCode().value()))
					.onErrorReturn(HttpStatus.SERVICE_UNAVAILABLE).subscribeOn(Schedulers.boundedElastic()).toFuture()
					.get(kafkaConfigData.getSchemaRegistryTimeout(), TimeUnit.SECONDS);
		} catch (TimeoutException e) {
			log.warn("Schema registry request timed out after " + kafkaConfigData.getSchemaRegistryTimeout()
					+ " seconds");
			return HttpStatus.SERVICE_UNAVAILABLE;
		} catch (Exception e) {
			return HttpStatus.SERVICE_UNAVAILABLE;
		}
	}

	/**
	 * @param sleepTimeMs
	 */
	private void sleep(Long sleepTimeMs) {
		try {
			Thread.sleep(sleepTimeMs);
		} catch (InterruptedException e) {
			throw new KafkaClientException("Error while sleeping for waiting new created topics!!");
		}

	}

	/**
	 * @param retry
	 * @param maxAttempts
	 */
	private void checkMaxRetry(int retry, Integer maxAttempts) {
		if (retry > maxAttempts) {
			throw new KafkaClientException("Reached max number of retry for reading kafka topic(s)!");
		}
	}

	/**
	 * @param topics
	 * @param topicName
	 * @return
	 */
	private boolean isTopicCreated(Collection<TopicListing> topics, String topicName) {
		return (topics == null) ? false : topics.stream().anyMatch(topic -> topic.name().equalsIgnoreCase(topicName));
	}

	/**
	 * @return
	 */
	private Collection<TopicListing> getTopics() {
		Collection<TopicListing> topics = null;
		try {
			topics = retryTemplate.execute(this::doGetTopics);
		} catch (Throwable t) {
			throw new KafkaClientException("Reached max number of retry for reading kafka topic(s)!", t);
		}
		return topics;
	}

	/**
	 * @param retryContext
	 * @return
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	private Collection<TopicListing> doGetTopics(RetryContext retryContext)
			throws InterruptedException, ExecutionException {
		log.info("Reading kafka topic {}, attempt {}", kafkaConfigData.getTopicNamesToCreate().toArray(),
				retryContext.getRetryCount());
		Collection<TopicListing> topics = adminClient.listTopics().listings().get();
		if (topics != null) {
			topics.forEach(topic -> log.debug("Topic with name {}", topic.name()));
		}
		return topics;
	}

	/**
	 * @param retryContext
	 * @return
	 */
	private CreateTopicsResult doCreateTopics(RetryContext retryContext) {
		List<String> topicNames = kafkaConfigData.getTopicNamesToCreate();
		log.info("Creating {} topics(s), attempt {}", topicNames.size(), retryContext.getRetryCount());
		List<NewTopic> kafkaTopics = topicNames.stream().map(topic -> new NewTopic(topic.trim(),
				kafkaConfigData.getNumOfPartitions(), kafkaConfigData.getReplicationFactor())).toList();
		return adminClient.createTopics(kafkaTopics);
	}
}
