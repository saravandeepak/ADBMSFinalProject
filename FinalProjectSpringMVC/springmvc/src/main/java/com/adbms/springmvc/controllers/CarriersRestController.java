package com.adbms.springmvc.controllers;

import java.util.ArrayList;

import javax.servlet.http.HttpServletRequest;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.adbms.springmvc.hbase.CarriersHbase;
import com.adbms.springmvc.pojo.Carrier;

@RestController
public class CarriersRestController {

	@RequestMapping(value = "/api/v1/getCarrierAgg", method = RequestMethod.GET, produces = "application/json")
	public ArrayList<Carrier>  getLocationAgg (HttpServletRequest request) {
		System.out.println("inside the rest controller");
		String cc = request.getParameter("cc");
		ArrayList<Carrier> carriersArray = new ArrayList<>();
		ArrayList<String> array = new ArrayList<>();
		array.add("2017Q2");
		array.add("2017Q1");
		array.add("2016Q4");
		array.add("2016Q3");
		array.add("2016Q2");
		array.add("2016Q1");
		
		
		for (String a: array) {
			Carrier carrier = CarriersHbase.getCarriersFromHbase(cc, a);
			carriersArray.add(carrier);
		}
		
		return carriersArray;
	}
}
