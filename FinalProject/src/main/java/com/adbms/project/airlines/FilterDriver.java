package com.adbms.project.airlines;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import com.adbms.project.airlines.filter.FilterMapper;

public class FilterDriver {
	
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: MaxSubmittedCharge <input path> <output path>");
			System.exit(-1);
		}
		BasicConfigurator.configure();
	
		Path inputPath = new Path(args[0]);
		Path outputDir = new Path(args[1]);
		Path mergeDir = new Path("filtermerge");
	
		// Create configuration
		Configuration conf = new Configuration(true);
	
		// Create job
		Job job = Job.getInstance(conf, "Filter");
		job.setJarByClass(FilterMapper.class);
		
	
		// Setup MapReduce
		job.setMapperClass(FilterMapper.class);
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
		FileSystem local = FileSystem.getLocal(conf);
		
		//merge all files
		try {
			FileStatus[] inputFiles = hdfs.listStatus(outputDir);
			FSDataOutputStream out = hdfs.create(mergeDir);
			
			for(int i = 0; i<inputFiles.length; i++) {
				System.out.println(inputFiles[i].getPath().getName());
				FSDataInputStream in = local.open(inputFiles[i].getPath());
				byte buffer[] = new byte[256];
				int bytesRead = 0;
				while ((bytesRead= in.read(buffer))>0) {
					out.write(buffer, 0, bytesRead);
				}
				in.close();
			}
			out.close();
		}
		catch (IOException e){
			e.printStackTrace();
		}
		System.exit(code);
	}
}
