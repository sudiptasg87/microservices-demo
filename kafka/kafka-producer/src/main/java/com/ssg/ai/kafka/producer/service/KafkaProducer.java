package com.ssg.ai.kafka.producer.service;

import java.io.Serializable;

import org.apache.avro.specific.SpecificRecordBase;

/**
 * @param <K>
 * @param <V>
 */
public interface KafkaProducer<K extends Serializable, V extends SpecificRecordBase> {
	
	/**
	 * @param topicName
	 * @param key
	 * @param message
	 */
	void send(String topicName, K key, V message);

}
