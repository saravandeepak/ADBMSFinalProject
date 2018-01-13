package com.adbms.springmvc.controllers;

import java.util.ArrayList;

import javax.servlet.http.HttpServletRequest;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.adbms.springmvc.hbase.LocationsHbase;
import com.adbms.springmvc.pojo.Location;

@RestController
public class LocationsRestController {

	@RequestMapping(value = "/api/v1/getLocationAgg", method = RequestMethod.GET, produces = "application/json")
	public ArrayList<Location>  getLocationAgg (HttpServletRequest request) {
		System.out.println("inside the rest controller");
		String src = request.getParameter("src");
		String des = request.getParameter("des");
		ArrayList<Location> locationsArray = new ArrayList<>();
		ArrayList<String> array = new ArrayList<>();
		array.add("2017Q2");
		array.add("2017Q1");
		array.add("2016Q4");
		array.add("2016Q3");
		array.add("2016Q2");
		array.add("2016Q1");
		
		for (String a: array) {
			Location location = LocationsHbase.getLocationsFromHbase(src, des, a);
			locationsArray.add(location);
		}
		
		return locationsArray;
	}
}
