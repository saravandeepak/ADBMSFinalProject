package com.adbms.project.airlines.locations.aggregation;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.mapreduce.Reducer;

import com.adbms.project.airlines.locations.writables.AggCustomWritable;
import com.adbms.project.airlines.locations.writables.LocationCustomWritable;
import com.adbms.project.hbase.HBaseConf;
import com.adbms.project.hbase.LocationsHbase;



public class LocationAggReducer extends Reducer<LocationCustomWritable, AggCustomWritable, LocationCustomWritable, AggCustomWritable>{

	private AggCustomWritable acw  = new AggCustomWritable();
	private Table table = null;
	private Connection conn = null;
	@Override
	protected void setup(
			Reducer<LocationCustomWritable, AggCustomWritable, LocationCustomWritable, AggCustomWritable>.Context context)
			throws IOException, InterruptedException {
		conn = HBaseConf.getConnection();
		table = HBaseConf.getLocationsTable(conn);
	}


	@Override
	protected void reduce(LocationCustomWritable key, Iterable<AggCustomWritable> values,
			Reducer<LocationCustomWritable, AggCustomWritable, LocationCustomWritable, AggCustomWritable>.Context context)
			throws IOException, InterruptedException {
		
		float sum = 0;
		float count = 0;
		float min = 100000f;
		float max = 0f;

		for(AggCustomWritable ac : values) {
			sum += ac.getAvg() * ac.getCount();
			count += ac.getCount();
			if(ac.getMin() < min) {
				min = ac.getMin();
			}
			
			if(ac.getMax() > max ) {
				max = ac.getMax();
			}
		}
		
		acw.setCount(count);
		acw.setAvg(sum/count);
		acw.setMax(max);
		acw.setMin(min);
		acw.setSum(Math.round(sum));
		LocationsHbase.addLocationsToHbase(table, "2016Q4", key.toString(), acw.getMin(), acw.getMax(), acw.getAvg());
		context.write(key, acw);
		
	}


	@Override
	protected void cleanup(
			Reducer<LocationCustomWritable, AggCustomWritable, LocationCustomWritable, AggCustomWritable>.Context context)
			throws IOException, InterruptedException {
		HBaseConf.closeConnection(conn, table);
	}
	
	
	
}
