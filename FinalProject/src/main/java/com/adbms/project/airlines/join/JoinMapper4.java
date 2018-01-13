package com.adbms.project.airlines.join;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinMapper4 extends Mapper<LongWritable, Text, LongWritable, Text> {
	
	private LongWritable outKey = new LongWritable();
	private Text outValue = new Text();
	private LongWritable l = new LongWritable(100000);

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {
		String fields[] = value.toString().split(",");

		try {
			Long itinId = Long.parseLong(fields[0]);
			outKey.set(itinId);
			outValue.set("B" + value.toString());
			context.write(outKey, outValue);
		}
		catch (Exception e) {
			outValue.set("B" + value.toString());
			context.write(l, outValue);
			e.printStackTrace();
		}
		
	}
}
