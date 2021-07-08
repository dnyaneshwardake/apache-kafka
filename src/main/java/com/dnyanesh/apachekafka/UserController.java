package com.dnyanesh.apachekafka.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.dnyanesh.apachekafka.dto.EmailRequest;
import com.dnyanesh.apachekafka.producer.KafkaProducer;

import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping("/kafka")
@Slf4j
public class UserController {

	@Autowired
	private KafkaProducer producer;

	@PostMapping("/send-email")
	public String sendEmailRequest(@RequestBody EmailRequest emailRequest) {
		log.info("Email message data :: " + emailRequest);
		producer.publishEmailRequestMessage(emailRequest);
		return "success";
	}

	@GetMapping
	public EmailRequest test() {
		return new EmailRequest();
	}
}
