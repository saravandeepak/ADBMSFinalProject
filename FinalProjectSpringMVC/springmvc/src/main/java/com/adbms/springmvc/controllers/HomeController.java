package com.adbms.springmvc.controllers;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class HomeController {

	@GetMapping(value= {"/", "home.htm"})
	public String index() {
		return "index";
	}
	
}
