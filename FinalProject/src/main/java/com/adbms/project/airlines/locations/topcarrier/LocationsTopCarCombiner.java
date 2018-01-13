package com.adbms.project.airlines.locations.topcarrier;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

import com.adbms.project.airlines.locations.writables.LocationCustomWritable;

public class LocationsTopCarCombiner extends Reducer<LocationCustomWritable, SortedMapWritable, LocationCustomWritable, SortedMapWritable> {

	@Override
	protected void reduce(LocationCustomWritable key, Iterable<SortedMapWritable> values,
			Reducer<LocationCustomWritable, SortedMapWritable, LocationCustomWritable, SortedMapWritable>.Context context)
			throws IOException, InterruptedException {
		SortedMapWritable sortedMap = new SortedMapWritable();
		for (SortedMapWritable val : values) {
			for (Entry<WritableComparable, Writable> entry : val.entrySet()) {
				LongWritable count = (LongWritable) sortedMap.get(entry.getKey());
				
				if (count != null ) {
					count.set(count.get() + ((LongWritable) entry.getValue()).get());	
				} else {
					sortedMap.put(entry.getKey(), new LongWritable(((LongWritable) entry.getValue()).get()));
				}
			}
			val.clear();
		}
		
		context.write(key, sortedMap);
	}

}
