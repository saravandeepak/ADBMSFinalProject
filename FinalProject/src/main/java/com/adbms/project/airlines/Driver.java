package com.adbms.project.airlines;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.BasicConfigurator;

import com.adbms.project.airlines.filter.FilterMapper;
import com.adbms.project.airlines.join.JoinMapper1;
import com.adbms.project.airlines.join.JoinMapper2;
import com.adbms.project.airlines.join.JoinMapper3;
import com.adbms.project.airlines.join.JoinMapper4;
import com.adbms.project.airlines.join.JoinReducer1;
import com.adbms.project.airlines.join.JoinReducer2;

public class Driver {
private static final String INTER_PATH = "intermediate_output";
	
	public static void main(String[] args) throws Exception {
		
		if (args.length != 4) {
			System.err.println("Usage: MaxSubmittedCharge <input path> <output path>");
			System.exit(-1);
		}
		
		BasicConfigurator.configure();

		Path file1Path = new Path(args[0]);
		Path file2Path = new Path(args[1]);
		Path file3Path = new Path(args[2]);
		Path interDir = new Path(INTER_PATH);
		Path outputDir = new Path(args[3]);

		// Create configuration
		Configuration conf = new Configuration(true);
		conf.set(TextOutputFormat.SEPERATOR, ",");

		// Create job
		Job job = Job.getInstance(conf, "Job1");
		job.setJarByClass(JoinMapper1.class);
		job.setNumReduceTasks(1);
		// Setup MapReduce
		MultipleInputs.addInputPath(job, file1Path, TextInputFormat.class, JoinMapper1.class);
		MultipleInputs.addInputPath(job, file2Path, TextInputFormat.class, JoinMapper2.class);
		job.setReducerClass(JoinReducer1.class);

		// Specify key / value
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		

		// Output
		FileOutputFormat.setOutputPath(job, interDir);
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(interDir))
			hdfs.delete(interDir, true);
		
		job.waitForCompletion(true);
//	    System.exit(code);

		// Execute job
		
		Configuration conf2 = new Configuration(true);
		Job job2 = Job.getInstance(conf2, "Job 2");
	    job2.setJarByClass(JoinMapper2.class);
	    
	    job2.setNumReduceTasks(1);
	    MultipleInputs.addInputPath(job2, interDir, TextInputFormat.class, JoinMapper3.class);
		MultipleInputs.addInputPath(job2, file3Path, TextInputFormat.class, JoinMapper4.class);
	    job2.setReducerClass(JoinReducer2.class);
	    
	    job2.setMapOutputKeyClass(LongWritable.class);
		job2.setMapOutputValueClass(Text.class);
	
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(Text.class);
	
	    job2.setOutputFormatClass(TextOutputFormat.class);
	
	    FileOutputFormat.setOutputPath(job2, outputDir);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);
	
	    int code1 =  job2.waitForCompletion(true) ? 0 : 1;
	    System.exit(code1);
		
	}
}
