package com.adbms.project.airlines.locations.topk;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.adbms.project.airlines.locations.writables.LocationCustomWritable;

public class LocationTopCountMapper extends Mapper<LongWritable, Text, LocationCustomWritable, LongWritable> {

	private LocationCustomWritable lcw = new LocationCustomWritable();
	private LongWritable one = new LongWritable(1);
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, LocationCustomWritable, LongWritable>.Context context)
			throws IOException, InterruptedException {
		
		String fields[] = value.toString().split(",");
	
		lcw.setOrigin(fields[16]);
		lcw.setDestination(fields[20]);
		
		context.write(lcw, one);
	}

}
