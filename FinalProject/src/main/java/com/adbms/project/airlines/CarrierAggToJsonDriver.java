package com.adbms.project.airlines;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import com.adbms.project.airlines.carriers.tojson.CarrierAggToJson;
import com.adbms.project.airlines.locations.tojson.LocationsToJsonMapper;

public class CarrierAggToJsonDriver {
public static void main(String[] args) throws Exception {
		
		BasicConfigurator.configure();
	
		
		Path inputPath = new Path("carrierAggOutput/part-r-00000");
		Path outputDir = new Path("CarrierAggJson");
	
		// Create configuration
		Configuration conf = new Configuration(true);
	
		// Create job
		Job job = Job.getInstance(conf, "CarrierAggToJson");
		job.setJarByClass(CarrierAggToJson.class);
		
	
		// Setup MapReduce
		job.setMapperClass(CarrierAggToJson.class);
		job.setNumReduceTasks(0);
	
		// Specify key / value
	
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
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
