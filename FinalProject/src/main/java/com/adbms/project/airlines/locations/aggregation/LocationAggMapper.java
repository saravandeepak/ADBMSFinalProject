package com.adbms.project.airlines.locations.aggregation;



import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.adbms.project.airlines.locations.writables.AggCustomWritable;
import com.adbms.project.airlines.locations.writables.LocationCustomWritable;

public class LocationAggMapper extends Mapper<LongWritable, Text, LocationCustomWritable, AggCustomWritable> {

	private LocationCustomWritable lcw = new LocationCustomWritable();
	private AggCustomWritable acw = new AggCustomWritable();
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, LocationCustomWritable, AggCustomWritable>.Context context)
			throws IOException, InterruptedException {
		
		String fields[] = value.toString().split(",");
		
		try {
			lcw.setOrigin(fields[16]);
			lcw.setDestination(fields[20]);
			acw.setCount(1);
			acw.setAvg(Float.parseFloat(fields[28]));
			acw.setMax(Float.parseFloat(fields[28]));
			acw.setMin(Float.parseFloat(fields[28]));
			acw.setSum(0l);
			context.write(lcw, acw);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	
	
}
