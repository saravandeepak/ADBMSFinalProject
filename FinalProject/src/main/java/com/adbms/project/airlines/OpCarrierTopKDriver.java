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

import com.adbms.project.airlines.opcarrier.topk.OpCarrierTopCountMapper;
import com.adbms.project.airlines.opcarrier.topk.OpCarrierTopCountReducer;
import com.adbms.project.airlines.opcarrier.topk.OpCarrierTopKMapper;
import com.adbms.project.airlines.opcarrier.topk.OpCarrierTopKReducer;
import com.adbms.project.airlines.sort.DescendingSortComparator;

public class OpCarrierTopKDriver {
public static void main(String[] args) throws Exception {
		
		BasicConfigurator.configure();

		Path inputPath = new Path("2016Q4/filtermerge");
		Path interDir = new Path("2016Q4/topKOpCarrier_intermiadte_output");
		Path outputDir = new Path("2016Q4/topkOpCarrier");

		// Create configuration
		Configuration conf = new Configuration(true);

		// Create job
		Job job = Job.getInstance(conf, "Job1");
		job.setJarByClass(OpCarrierTopCountMapper.class);
		
		
		job.setNumReduceTasks(1);
		// Setup MapReduce
		job.setMapperClass(OpCarrierTopCountMapper.class);
		job.setReducerClass(OpCarrierTopCountReducer.class);
		
		// Specify key / value
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setOutputKeyClass(Text.class);
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
	    job2.setJarByClass(OpCarrierTopKMapper.class);
	    
	    job2.setNumReduceTasks(1);
	    job2.setSortComparatorClass(DescendingSortComparator.class);
	    job2.setMapperClass(OpCarrierTopKMapper.class);
	    job2.setReducerClass(OpCarrierTopKReducer.class);
	
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
