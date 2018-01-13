package com.adbms.project.airlines;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import com.adbms.project.airlines.locations.aggregation.LocationAggMapper;
import com.adbms.project.airlines.locations.aggregation.LocationAggReducer;
import com.adbms.project.airlines.locations.writables.AggCustomWritable;
import com.adbms.project.airlines.locations.writables.LocationCustomWritable;

public class LocationAggDriver {
	
	public static void main(String[] args) throws Exception {
		
		BasicConfigurator.configure();
	
		Path inputPath = new Path("2016Q4/filtermerge");
		Path outputDir = new Path("2016Q4/locations/aggOutput");
	
		// Create configuration
		Configuration conf = new Configuration(true);
	
		// Create job
		Job job = Job.getInstance(conf, "LocationAgg");
		job.setJarByClass(LocationAggMapper.class);
	
		// Setup MapReduce
		job.setMapperClass(LocationAggMapper.class);
		job.setCombinerClass(LocationAggReducer.class);
		job.setReducerClass(LocationAggReducer.class);
		job.setNumReduceTasks(1);
	
		// Specify key / value
	
		job.setMapOutputKeyClass(LocationCustomWritable.class);
		job.setMapOutputValueClass(AggCustomWritable.class);
		
		job.setOutputKeyClass(LocationCustomWritable.class);
		job.setOutputValueClass(AggCustomWritable.class);
		// Input
		FileInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(TextInputFormat.class);
	
		// Output
		FileOutputFormat.setOutputPath(job, outputDir);
	
		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);
	
		// Execute job
		int code = job.waitForCompletion(true) ? 0 : 1;
		System.exit(code);
	}

}
