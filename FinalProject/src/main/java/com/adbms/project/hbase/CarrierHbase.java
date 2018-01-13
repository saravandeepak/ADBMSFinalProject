package com.adbms.project.hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class CarrierHbase {
	
	public static Table getCarriersTable(Connection conn) {
		try {
			Table table = conn.getTable(TableName.valueOf(Bytes.toBytes("carriersAdbms")));
			return table;
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public static void addCarriersToHbase (Table table, String cf, String carrierCode, Float minFare, Float maxFare, Float avgFare, Long sum) {
		try {
			Put put = new Put (Bytes.toBytes(carrierCode));
			put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("minFare"), Bytes.toBytes(minFare.toString()));
			put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("maxFare"), Bytes.toBytes(maxFare.toString()));
			put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("avgFare"), Bytes.toBytes(avgFare.toString()));
			put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("sumFare"), Bytes.toBytes(sum.toString()));
			table.put(put);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}


}
