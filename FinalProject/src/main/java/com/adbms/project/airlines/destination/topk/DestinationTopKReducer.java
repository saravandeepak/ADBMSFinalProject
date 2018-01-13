package com.adbms.project.airlines.destination.topk;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class DestinationTopKReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

	private int count = 0;
	JsonArray topDestinations = new JsonArray();
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		count = 0;
	}

	public void reduce(Text key, Iterable<NullWritable> values,Context context)
			throws IOException, InterruptedException {

		if (count < 25) {
			JsonObject destination = nestElements(key.toString());
			topDestinations.add(destination);
			//context.write(key, NullWritable.get());
			count ++;
		}
		
	} 
	
	private JsonObject nestElements(String originTopK) {
		try{
			
			JsonObject destination = new JsonObject();
			String fields[] = originTopK.toString().split(" ");
			String des = fields[1].substring(1, fields[1].length()-1);
			destination.addProperty("destination", des);
			Long count = Long.parseLong(fields[0]);
			destination.addProperty("count", count);
			return destination;
		}
		catch(Exception e) {
			e.printStackTrace();
			
		}
		return null;
	}

	@Override
	protected void cleanup(Reducer<Text, NullWritable, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		Gson gson = new GsonBuilder().setPrettyPrinting().serializeNulls().setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE).create();
		context.write(new Text(gson.toJson(topDestinations)), NullWritable.get());
	}


}
