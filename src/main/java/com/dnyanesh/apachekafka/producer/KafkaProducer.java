package com.dnyanesh.apachekafka.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.dnyanesh.apachekafka.dto.EmailRequest;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class KafkaProducer {

	@Value("${EMAIL_TOPIC_NAME}")
	private String email_topic;

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	public void publishEmailRequestMessage(EmailRequest emailRequest) {
		JSONObject jsonString = new JSONObject(emailRequest);
		ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(email_topic,
				jsonString.toString());	

		kafkaTemplate.send(producerRecord);
		log.info("Published EmailRequest message :: " + jsonString);
	}
}
