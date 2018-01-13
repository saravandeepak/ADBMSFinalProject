package com.adbms.springmvc.hbase;

import java.util.ArrayList;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.adbms.springmvc.pojo.Location;
import org.apache.hadoop.hbase.client.Result;


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
	
	public static Location getLocationsFromHbase (String src, String des, String cf) {
		try{
			Connection conn = HBaseConf.getConnection();
			Table table = conn.getTable(TableName.valueOf( Bytes.toBytes("locationsAdbms")));
			Get get = new Get(Bytes.toBytes("\"" + src + "\"" + "-" + "\"" + des + "\""));
			Result result = table.get(get);
			HBaseConf.closeConnection(conn, table);
			NavigableMap<byte[], byte[]> map = result.getFamilyMap(Bytes.toBytes(cf));
			Location location = new Location();
			location.setSrc(src);
			location.setDes(des);
			location.setTime(cf);
	
			for(byte[] key : map.keySet()){
				if(Bytes.toString(key).equals("avgFare")) {
					location.setAvgFare(Float.parseFloat(Bytes.toString(map.get(key))));
				}  
				if(Bytes.toString(key).equals("minFare")) {
					location.setMinFare(Float.parseFloat(Bytes.toString(map.get(key))));
				}
				if(Bytes.toString(key).equals("maxFare")) {
					location.setMaxFare(Float.parseFloat(Bytes.toString(map.get(key))));
				}
				if(Bytes.toString(key).equals("carrierCode")) {
					location.setCc(Bytes.toString(map.get(key)));
				}
				
			}
			
			return location;
			
		}
		catch (Exception e) {
			
		}
		return null;
	}
}
