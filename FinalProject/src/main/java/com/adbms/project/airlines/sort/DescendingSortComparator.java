package com.adbms.project.airlines.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DescendingSortComparator extends WritableComparator {

  protected DescendingSortComparator() {
		super(Text.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		Text key1 = (Text) w1;
		Text key2 = (Text) w2;
		String[] fields1 = key1.toString().split(" ");
		int total1 = Integer.parseInt(fields1[0]);
		IntWritable t1 = new IntWritable(total1);
		
		String[] fields2 = key2.toString().split(" ");
		int total2 = Integer.parseInt(fields2[0]);
		IntWritable t2 = new IntWritable(total2);
		
		return -1 * t1.compareTo(t2);
	}
}
