package com.adbms.project.airlines.filter;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FilterMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	
	private Text outkey = new Text();

	static int count = 0;
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
	
		String fields[] = value.toString().split(",");
		String filteredString = fields[0] + "," + fields[1] + "," + fields[4] + "," + fields[8] + "," + fields[9] + "," 
		+ fields[10] + "," + fields[12] + "," + fields[13] + "," + fields[18] + "," + fields[19] + "," + fields[21] + "," 
		+ fields[22] + "," + fields[26] + "," + fields[27] + "," + fields[28] + "," + fields[29] + "," + fields[45] + "," 
		+ fields[46] + "," + fields[48] + "," + fields[49] + "," + fields[54] + "," + fields[55] + "," + fields[57] + "," 
		+ fields[58] + "," + fields[66] + "," + fields[67] + "," + fields[68] + ","  + fields[86] + "," + fields[71] 
		+ "," + fields[92] + "," + fields[94];
		outkey.set(filteredString);

		try {
			if (Float.parseFloat(fields[71]) > 30 && fields[9] != fields[18] && Float.parseFloat(fields[71]) < 3500) {
				context.write(outkey, NullWritable.get() );
			}
		}
		catch (Exception e) {
			context.write(outkey, NullWritable.get());
		}
		
		
	}
	
	

}
