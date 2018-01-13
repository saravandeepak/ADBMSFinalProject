package com.adbms.project.airlines.locations.topk;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LocationTopKMapper extends Mapper<Text, Text, Text, NullWritable> {

	@Override
	protected void map(Text key, Text value, Mapper<Text, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		
		Text outKey = new Text(value.toString() + " " + key.toString());
		context.write(outKey, NullWritable.get());
		
	}
}
