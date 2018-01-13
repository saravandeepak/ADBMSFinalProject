package com.adbms.project.airlines.opcarrier.topk;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OpCarrierTopCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	private LongWritable total = new LongWritable();
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		
		Long sum = 0l;
		for (LongWritable l : values) {
			sum += l.get();
		}
		total.set(sum);
		context.write(key, total);
	}

}
