package com.adbms.springmvc;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

@SpringBootApplication
public class SpringmvcApplication {

	protected SpringApplicationBuilder configure (SpringApplicationBuilder application) {
		return application.sources(SpringmvcApplication.class);
	}
	public static void main(String[] args) {
		SpringApplication.run(SpringmvcApplication.class, args);
	}
}
