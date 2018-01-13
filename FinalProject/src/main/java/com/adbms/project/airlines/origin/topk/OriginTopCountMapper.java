package com.adbms.project.airlines.origin.topk;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OriginTopCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	private Text outKey = new Text();
	private LongWritable one = new LongWritable(1);
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		
		String fields[] = value.toString().split(",");
		
		outKey.set(fields[16]);
		context.write(outKey, one);
	}


}
