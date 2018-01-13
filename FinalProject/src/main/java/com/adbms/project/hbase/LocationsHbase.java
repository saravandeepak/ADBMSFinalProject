package com.adbms.project.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;


public class LocationsHbase {
	public static void addLocationsToHbase (Table table, String cf, String srcDes, Float minFare, Float maxFare, Float avgFare) {
		try {
			Put put = new Put (Bytes.toBytes(srcDes));
			put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("minFare"), Bytes.toBytes(minFare.toString()));
			put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("maxFare"), Bytes.toBytes(maxFare.toString()));
			put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("avgFare"), Bytes.toBytes(avgFare.toString()));
			table.put(put);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void addTopCarrierToHbase (Table table, String cf, String srcDes, String carrierCode) {
		try {
			Put put = new Put (Bytes.toBytes(srcDes));
			put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("carrierCode"), Bytes.toBytes(carrierCode));
			table.put(put);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}
