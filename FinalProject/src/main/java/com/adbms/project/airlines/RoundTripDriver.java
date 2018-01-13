package com.adbms.project.airlines;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.counters.Limits;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import com.adbms.project.airlines.locations.tojson.LocationsToJsonMapper;
import com.adbms.project.airlines.roundtrip.RoundTripMapper;

public class RoundTripDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setInt(MRJobConfig.COUNTERS_MAX_KEY, 6000);
		BasicConfigurator.configure();
		Limits.init(conf);

		
		Path input = new Path("2016Q2/filtermerge");
		Path outputDir = new Path("2016Q2/RoundtripCounter");

		Job job = Job.getInstance(conf, "Count IP Address");
		job.setJarByClass(RoundTripMapper.class);
		

		job.setMapperClass(RoundTripMapper.class);
		job.setNumReduceTasks(0);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, outputDir);
		int code = job.waitForCompletion(true) ? 0 : 1;

		if (code == 0) {
			for (Counter counter : job.getCounters().getGroup(
					RoundTripMapper.ROUNDTRIP)) {
				System.out.println(counter.getDisplayName() + "\t"
						+ counter.getValue());
			}
		}
		
		FileSystem.get(conf).delete(outputDir, true);

		System.exit(code);
	}
}
