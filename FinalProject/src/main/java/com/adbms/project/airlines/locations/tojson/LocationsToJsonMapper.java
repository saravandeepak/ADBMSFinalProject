package com.adbms.project.airlines.locations.tojson;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashSet;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class LocationsToJsonMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	static int count = 0;
	JsonArray locations  = new JsonArray();
	JsonObject jsonOut;

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		JsonElement location = nestElements(value.toString());
		locations.add(location);
	}
	
	private JsonObject nestElements(String locationsAgg) {
		try{
			JsonObject location = new JsonObject();
			String fields[] = locationsAgg.toString().split("\t");
			String orgDes[] = fields[0].split("-") ;
			String org = orgDes[0].substring(1, orgDes[0].length()-1);;
			location.addProperty("origin", org);
			String des = orgDes[1].substring(1, orgDes[1].length()-1);
			location.addProperty("destination", des);
			String agg[] = fields[1].split("\\|");
			
			String minkv[] = agg[0].split(":");
			String maxkv[] = agg[1].split(":");
			String avgkv[] = agg[2].split(":");
			location.addProperty("minFare", Float.parseFloat(minkv[1]));
			location.addProperty("maxFare", Float.parseFloat(maxkv[1]));
			location.addProperty("avgFare", Float.parseFloat(avgkv[1]));
			
			return location;
		}
		catch(Exception e) {
			e.printStackTrace();
			
		}
		return null;
	}

	@Override
	protected void cleanup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		Gson gson = new GsonBuilder().setPrettyPrinting().serializeNulls().setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE).create();
		context.write(new Text(gson.toJson(locations)), NullWritable.get());
	}
	
	


	
}
