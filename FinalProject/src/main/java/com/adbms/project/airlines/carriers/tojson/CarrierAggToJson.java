package com.adbms.project.airlines.carriers.tojson;

import java.io.IOException;

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

public class CarrierAggToJson extends Mapper<LongWritable, Text, Text, NullWritable>{
	static int count = 0;
	JsonArray carriers  = new JsonArray();
	JsonObject jsonOut;

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		JsonElement carrier = nestElements(value.toString());
		carriers.add(carrier);
	}
	
	private JsonObject nestElements(String locationsAgg) {
		try{
			JsonObject carrier = new JsonObject();
			
			String fields[] = locationsAgg.toString().split("\t");
			String carrierCode = fields[0];
			String cc = carrierCode.substring(1, carrierCode.length()-1);;
			
			carrier.addProperty("carrierCode", cc);
			String agg[] = fields[1].split("\\|");
			String minkv[] = agg[0].split(":");
			String maxkv[] = agg[1].split(":");
			String avgkv[] = agg[2].split(":");
			String sumkv[] = agg[3].split(":");
			carrier.addProperty("minFare", Float.parseFloat(minkv[1]));
			carrier.addProperty("maxFare", Float.parseFloat(maxkv[1]));
			carrier.addProperty("avgFare", Float.parseFloat(avgkv[1]));
			carrier.addProperty("sumFare", Float.parseFloat(sumkv[1]));
			
			return carrier;
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
		context.write(new Text(gson.toJson(carriers)), NullWritable.get());
	}
	
}
