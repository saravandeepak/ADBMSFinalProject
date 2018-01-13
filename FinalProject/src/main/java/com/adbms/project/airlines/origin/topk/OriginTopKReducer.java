package com.adbms.project.airlines.origin.topk;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;


public class OriginTopKReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

	private int count = 0;
	JsonArray topOrigins = new JsonArray();
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		count = 0;
	}

	public void reduce(Text key, Iterable<NullWritable> values,Context context)
			throws IOException, InterruptedException {

		if (count < 25) {
			JsonObject origin = nestElements(key.toString());
			topOrigins.add(origin);
			//context.write(key, NullWritable.get());
			count ++;
		}
		
	} 
	
	private JsonObject nestElements(String originTopK) {
		try{
			
			JsonObject origin = new JsonObject();
			String fields[] = originTopK.toString().split(" ");
			String org = fields[1].substring(1, fields[1].length()-1);
			origin.addProperty("origin", org);
			Long count = Long.parseLong(fields[0]);
			origin.addProperty("count", count);
			return origin;
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
		context.write(new Text(gson.toJson(topOrigins)), NullWritable.get());
	}

	
	
	

}
