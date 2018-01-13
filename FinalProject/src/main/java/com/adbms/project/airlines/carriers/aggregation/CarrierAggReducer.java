package com.adbms.project.airlines.carriers.aggregation;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.adbms.project.airlines.locations.writables.AggCustomWritable;
import com.adbms.project.airlines.locations.writables.LocationCustomWritable;
import com.adbms.project.hbase.CarrierHbase;
import com.adbms.project.hbase.HBaseConf;

public class CarrierAggReducer extends Reducer<Text, AggCustomWritable, Text, AggCustomWritable>{

	private AggCustomWritable acw  = new AggCustomWritable();
	private Table table = null;
	private Connection conn = null;
	@Override
	protected void setup(
			Reducer<Text, AggCustomWritable, Text, AggCustomWritable>.Context context)
			throws IOException, InterruptedException {
		conn = HBaseConf.getConnection();
		table = CarrierHbase.getCarriersTable(conn);
	}
	
	@Override
	protected void reduce(Text key, Iterable<AggCustomWritable> values,
			Reducer<Text, AggCustomWritable, Text, AggCustomWritable>.Context context)
			throws IOException, InterruptedException {
		
		long sum = 0;
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
		acw.setSum(sum);
		CarrierHbase.addCarriersToHbase(table, "2016Q4", key.toString(), acw.getMin(), acw.getMax(), acw.getAvg(), acw.getSum());
		context.write(key, acw);
		
	}
	

}
