package com.adbms.project.airlines.locations.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class LocationCustomWritable implements WritableComparable<LocationCustomWritable> {
	
	private String origin;
	private String destination;

	public void readFields(DataInput dI) throws IOException {
		origin = WritableUtils.readString(dI);
		destination = WritableUtils.readString(dI);
	}

	public void write(DataOutput dO) throws IOException {
		WritableUtils.writeString(dO, origin);
		WritableUtils.writeString(dO, destination);
	}

	public int compareTo(LocationCustomWritable lc) {
		String cur = this.origin + this.destination;
		String prev = lc.origin + lc.destination;
		return cur.compareTo(prev);
	}

	public String getOrigin() {
		return origin;
	}

	public void setOrigin(String origin) {
		this.origin = origin;
	}

	public String getDestination() {
		return destination;
	}

	public void setDestination(String destination) {
		this.destination = destination;
	}

	@Override
	public String toString() {
		return this.origin + "-" + this.destination;
	}
	
	
	
	
	
	

}
