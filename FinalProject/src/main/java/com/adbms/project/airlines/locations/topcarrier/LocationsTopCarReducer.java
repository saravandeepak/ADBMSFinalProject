package com.adbms.project.airlines.locations.topcarrier;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

import com.adbms.project.airlines.locations.writables.AggCustomWritable;
import com.adbms.project.airlines.locations.writables.LocationCustomWritable;
import com.adbms.project.hbase.HBaseConf;
import com.adbms.project.hbase.LocationsHbase;

public class LocationsTopCarReducer extends Reducer<LocationCustomWritable, SortedMapWritable, LocationCustomWritable, Text> {

	private Table table = null;
	private Connection conn = null;
	@Override
	protected void setup(
			Reducer<LocationCustomWritable, SortedMapWritable, LocationCustomWritable, Text>.Context context)
			throws IOException, InterruptedException {
		conn = HBaseConf.getConnection();
		table = HBaseConf.getLocationsTable(conn);
	}
	
	@Override
	protected void reduce(LocationCustomWritable key, Iterable<SortedMapWritable> values,
			Reducer<LocationCustomWritable, SortedMapWritable, LocationCustomWritable, Text>.Context context)
			throws IOException, InterruptedException {
		Text airlines = new Text();
		Long max = 0L;
		for (SortedMapWritable val : values) {
			for (Entry<WritableComparable, Writable> entry : val.entrySet()) {
				if (((LongWritable) entry.getValue()).get() > max) {
					max = ((LongWritable) entry.getValue()).get();
					airlines.set(entry.getKey().toString() + " : " + entry.getValue().toString());
				}
			}
		}
		LocationsHbase.addTopCarrierToHbase(table, "2016Q4", key.toString(), airlines.toString());
		context.write(key, airlines);
		
	}
	
	@Override
	protected void cleanup(
			Reducer<LocationCustomWritable, SortedMapWritable, LocationCustomWritable, Text>.Context context)
			throws IOException, InterruptedException {
		HBaseConf.closeConnection(conn, table);
	}

	
}
