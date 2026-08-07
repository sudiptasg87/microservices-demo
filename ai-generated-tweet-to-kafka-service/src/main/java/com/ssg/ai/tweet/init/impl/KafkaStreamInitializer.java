package com.ssg.ai.tweet.init.impl;

import org.springframework.stereotype.Component;

import com.ssg.ai.tweet.init.StreamInitializer;

@Component
public class KafkaStreamInitializer implements StreamInitializer {

	/**
	 *
	 */
	@Override
	public boolean init() {
		return true;
	}

	
}
