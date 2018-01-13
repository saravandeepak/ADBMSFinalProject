package com.adbms.project.airlines.roundtrip;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RoundTripMapper extends Mapper<Object, Text, Text, NullWritable> {
	
	public static final String ROUNDTRIP = "Round_Trip";
	
	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		try {
			String[] fields = value.toString().split(",");
	        //context.write(t1, NullWritable.get());
	        if (Float.parseFloat(fields[27]) == 1.0) {
	        	context.getCounter(ROUNDTRIP, "roundtrip").increment(1);
	        }
	        context.getCounter(ROUNDTRIP, "sum").increment(1);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
	}

}
