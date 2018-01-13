package com.adbms.project.airlines.locations.topk;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.adbms.project.airlines.locations.writables.LocationCustomWritable;

public class LocationTopCountReducer extends Reducer<LocationCustomWritable, LongWritable, LocationCustomWritable, LongWritable> {

	private LongWritable total = new LongWritable();
	@Override
	protected void reduce(LocationCustomWritable key, Iterable<LongWritable> values,
			Reducer<LocationCustomWritable, LongWritable, LocationCustomWritable, LongWritable>.Context context)
			throws IOException, InterruptedException {
		
		Long sum = 0l;
		for (LongWritable l : values) {
			sum += l.get();
		}
		total.set(sum);
		context.write(key, total);
	}
	
}
