package com.adbms.project.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseConf {
	
	public static Connection getConnection() {
		Connection conn = null;
		try {
			Configuration conf = HBaseConfiguration.create();
			conn = ConnectionFactory.createConnection(conf);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
		if (conn != null) {
			System.out.println("Successfull connection");
		} else {
			System.out.println("Connection failed");
		}
		return conn;
	}
	
	public static void closeConnection (Connection conn, Table table) {
		try {
		conn.close();
		table.close();
		System.out.println("Connection closed");
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static Table getLocationsTable(Connection conn) {
		try {
			Table table = conn.getTable(TableName.valueOf(Bytes.toBytes("locationsAdbms")));
			return table;
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

}
