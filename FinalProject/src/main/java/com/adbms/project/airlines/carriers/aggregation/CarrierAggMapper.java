package com.adbms.project.airlines.carriers.aggregation;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.adbms.project.airlines.locations.writables.AggCustomWritable;


public class CarrierAggMapper extends Mapper<LongWritable, Text, Text, AggCustomWritable> {

	private AggCustomWritable acw = new AggCustomWritable();
	private Text outKey = new Text();
	
	static int count = 0;
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, AggCustomWritable>.Context context)
			throws IOException, InterruptedException {
		
		String fields[] = value.toString().split(",");
		Long sum = (long) Math.round(Float.parseFloat(fields[28])); 
		try {
			
			outKey.set(fields[26]);
			acw.setCount(1);
			acw.setAvg(Float.parseFloat(fields[28]));
			acw.setMax(Float.parseFloat(fields[28]));
			acw.setMin(Float.parseFloat(fields[28]));
			acw.setSum(sum);
			context.write(outKey, acw);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
	}


}
