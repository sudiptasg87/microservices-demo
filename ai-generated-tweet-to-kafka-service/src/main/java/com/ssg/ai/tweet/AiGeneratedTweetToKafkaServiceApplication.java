package com.ssg.ai.tweet;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages =  "com.ssg.ai")
public class AiGeneratedTweetToKafkaServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(AiGeneratedTweetToKafkaServiceApplication.class, args);
	}

}
