package com.adbms.project.airlines;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;
import org.apache.log4j.BasicConfigurator;

public class BloomFilterDriver {
	public static void main (String[] args) throws Exception {
		// Create configuration
		Configuration conf = new Configuration(true);
		Path outputDir = new Path("bloom/hotvalues");
		BasicConfigurator.configure();
		BloomFilter filter = new org.apache.hadoop.util.bloom.BloomFilter(2876, 20, Hash.MURMUR_HASH);
		File file1 = new File("topkLocations/part-r-00000");
		try(BufferedReader br = new BufferedReader(new FileReader(file1))) {
		    for(String line; (line = br.readLine()) != null; ) {
		    	System.out.println("lines" + line);
		        String fields[] =  line.split(" ");
		        String orgDes = fields[1];
		        filter.add(new Key(orgDes.getBytes()));
		    }
		    // line is not visible here.
		}
		
		System.setProperty("hadoop.home.dir", "/");
		FileSystem fs = FileSystem.getLocal(conf);
		
		
		if (fs.exists(outputDir))
			fs.delete(outputDir, true);
		
		FSDataOutputStream strm = fs.create(outputDir);
		filter.write(strm);
		strm.flush();
		strm.close();
	}

}
