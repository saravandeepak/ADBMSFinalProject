package com.adbms.project.airlines.tktcarrier.toprev;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TktCarrierTopRevCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	private Text outKey = new Text();
	private LongWritable outValue = new LongWritable(1);
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		try{
			String fields[] = value.toString().split(",");
			
			outKey.set(fields[25]);
			outValue.set((long) Math.round(Float.parseFloat(fields[28])));
			context.write(outKey, outValue);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}


} 


