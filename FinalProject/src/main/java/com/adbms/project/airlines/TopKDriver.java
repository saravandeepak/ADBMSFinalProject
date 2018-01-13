package com.adbms.project.airlines;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.BasicConfigurator;

import com.adbms.project.airlines.locations.topk.LocationTopCountMapper;
import com.adbms.project.airlines.locations.topk.LocationTopCountReducer;
import com.adbms.project.airlines.locations.topk.LocationTopKMapper;
import com.adbms.project.airlines.locations.topk.LocationTopKReducer;
import com.adbms.project.airlines.locations.writables.LocationCustomWritable;
import com.adbms.project.airlines.sort.DescendingSortComparator;

public class TopKDriver {

private static final String OUTPUT_PATH = "2016Q4/topkLocations_intermediate_output";
	
	public static void main(String[] args) throws Exception {
		
		BasicConfigurator.configure();

		Path inputPath = new Path("2016Q4/filtermerge");
		Path interDir = new Path(OUTPUT_PATH);
		Path outputDir = new Path("2016Q4/topkLocations");

		// Create configuration
		Configuration conf = new Configuration(true);

		// Create job
		Job job = Job.getInstance(conf, "Job1");
		job.setJarByClass(LocationTopCountMapper.class);
		
		
		job.setNumReduceTasks(1);
		// Setup MapReduce
		job.setMapperClass(LocationTopCountMapper.class);
		//job.setCombinerClass(MovieCombiner.class);
		job.setReducerClass(LocationTopCountReducer.class);
		
		
		// Specify key / value
	
		job.setMapOutputKeyClass(LocationCustomWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setOutputKeyClass(LocationCustomWritable.class);
		job.setOutputValueClass(LongWritable.class);
		// Input
		FileInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(TextInputFormat.class);

		// Output
		FileOutputFormat.setOutputPath(job, interDir);
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(interDir))
			hdfs.delete(interDir, true);

		// Execute job
		job.waitForCompletion(true);
		
		Job job2 = Job.getInstance(conf, "Job 2");
	    job2.setJarByClass(LocationTopKMapper.class);
	    
	    job2.setNumReduceTasks(1);
	    job2.setSortComparatorClass(DescendingSortComparator.class);
	    job2.setMapperClass(LocationTopKMapper.class);
	    job2.setReducerClass(LocationTopKReducer.class);
	
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(NullWritable.class);
	
	    job2.setInputFormatClass(KeyValueTextInputFormat.class);
	    job2.setOutputFormatClass(TextOutputFormat.class);
	
	    FileInputFormat.addInputPath(job2, interDir);
	    FileOutputFormat.setOutputPath(job2, outputDir);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);
	
	    int code =  job2.waitForCompletion(true) ? 0 : 1;
	    System.exit(code);
	}
}
