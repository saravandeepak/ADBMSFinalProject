package com.adbms.springmvc.hbase;

import java.util.NavigableMap;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.adbms.springmvc.pojo.Carrier;

public class CarriersHbase {
	public static Carrier getCarriersFromHbase (String cc, String cf) {
		try{
			Connection conn = HBaseConf.getConnection();
			Table table = conn.getTable(TableName.valueOf( Bytes.toBytes("carriersAdbms")));
			Get get = new Get(Bytes.toBytes("\"" + cc + "\""));
			Result result = table.get(get);
			HBaseConf.closeConnection(conn, table);
			NavigableMap<byte[], byte[]> map = result.getFamilyMap(Bytes.toBytes(cf));
			Carrier carrier = new Carrier();
			carrier.setCc(cc);
			carrier.setTime(cf);
			
			for(byte[] key : map.keySet()){
				if(Bytes.toString(key).equals("avgFare")) {
					carrier.setAvgFare(Float.parseFloat(Bytes.toString(map.get(key))));
				}  
				if(Bytes.toString(key).equals("minFare")) {
					carrier.setMinFare(Float.parseFloat(Bytes.toString(map.get(key))));
				}
				if(Bytes.toString(key).equals("maxFare")) {
					carrier.setMaxFare(Float.parseFloat(Bytes.toString(map.get(key))));
				}
				if(Bytes.toString(key).equals("sumFare")) {
					carrier.setSumFare(Long.parseLong(Bytes.toString(map.get(key))));
				}
				
			}
			
			return carrier;
			
		}
		catch (Exception e) {
			
		}
		return null;
	}
}
