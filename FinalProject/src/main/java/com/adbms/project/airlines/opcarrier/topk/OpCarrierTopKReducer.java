package com.adbms.project.airlines.opcarrier.topk;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;


public class OpCarrierTopKReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

	private int count = 0;
	JsonArray opCarriers = new JsonArray();
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		count = 0;
	}

	public void reduce(Text key, Iterable<NullWritable> values,Context context)
			throws IOException, InterruptedException {

		if (count < 25) {
			opCarriers.add(nestElements(key.toString()));
			//context.write(key, NullWritable.get());
			count ++;
		}
		
	}
	
	private JsonObject nestElements(String opCarrierTopK) {
		try{
			
			JsonObject opCar = new JsonObject();
			String fields[] = opCarrierTopK.toString().split(" ");
			String op = fields[1].substring(1, fields[1].length()-1);
			opCar.addProperty("opCarrier", op);
			Long count = Long.parseLong(fields[0]);
			opCar.addProperty("count", count);
			return opCar;
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
		context.write(new Text(gson.toJson(opCarriers)), NullWritable.get());
	}

}
