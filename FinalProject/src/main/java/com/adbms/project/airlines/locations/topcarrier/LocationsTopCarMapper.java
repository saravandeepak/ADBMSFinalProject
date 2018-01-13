package com.adbms.project.airlines.locations.topcarrier;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.adbms.project.airlines.locations.writables.LocationCustomWritable;

public class LocationsTopCarMapper extends Mapper<LongWritable, Text, LocationCustomWritable, SortedMapWritable> {

	private LocationCustomWritable lcw = new LocationCustomWritable();
	private SortedMapWritable sm = new SortedMapWritable();
	private static LongWritable one = new LongWritable(1);
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, LocationCustomWritable, SortedMapWritable>.Context context)
			throws IOException, InterruptedException {
		
		String fields[] = value.toString().split(",");
		
		try {
			lcw.setOrigin(fields[16]);
			lcw.setDestination(fields[20]);
			sm.put(new Text(fields[26]), one);
			
			context.write(lcw, sm);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
	}


}
