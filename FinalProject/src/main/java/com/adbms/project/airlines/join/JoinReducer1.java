package com.adbms.project.airlines.join;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer1 extends Reducer<LongWritable, Text, Text, Text> {
	
	private ArrayList<Text> listA = new ArrayList<Text>();
	private ArrayList<Text> listB = new ArrayList<Text>();

	@Override
	protected void reduce(LongWritable key, Iterable<Text> values, Reducer<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		listA.clear();
		listB.clear();
		for (Text value: values) {
			if(value.charAt(0)== 'A'){
				listA.add(new Text(value.toString().substring(1)));
			} else if(value.charAt(0)== 'B'){
				listB.add(new Text(value.toString().substring(1)));
			}
		}
		
		if(!listA.isEmpty() && !listB.isEmpty()) {
			for (Text a: listA){
				for (Text b: listB){
					context.write(a, b);
				}
			}
		}
		
	}
	
	

}
