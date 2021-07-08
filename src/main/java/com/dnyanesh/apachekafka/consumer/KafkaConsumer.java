package com.dnyanesh.apachekafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.dnyanesh.apachekafka.dto.EmailRequest;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class KafkaConsumer {

	@Value("${EMAIL_TOPIC_NAME}")
	private String EMAIL_TOPIC;

	@KafkaListener(id = "emailRequest", topics = "email_topic", groupId = "group_id")
	public void consumeEmailRequestMessage(ConsumerRecord<String, String> data) {
		log.info("Consumed EmailRequest message :: " + data.value());
		ObjectMapper mapper = new ObjectMapper();
		EmailRequest emailRequest = null;
		JSONObject jsonString = new JSONObject(data.value());
		try {
			emailRequest = mapper.readValue(jsonString.toString(), EmailRequest.class);
		} catch (Exception e) {
			log.error("Exception while parsing the JSON to EmailRequest");
		}
		log.info("Converted Java object :: " + emailRequest);
	}

}
