package com.adbms.project.airlines.locations.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class AggCustomWritable implements Writable {
	
	private float avg;
	private float min;
	private float max;
	private float count;
	private Long sum;
	private float stdDev;

	public void readFields(DataInput dI) throws IOException {
		avg = Float.parseFloat(WritableUtils.readString(dI));
		min = Float.parseFloat(WritableUtils.readString(dI));
		max = Float.parseFloat(WritableUtils.readString(dI));
		count = Float.parseFloat(WritableUtils.readString(dI));
		sum = (long) Math.round(Float.parseFloat(WritableUtils.readString(dI)));
		stdDev = Float.parseFloat(WritableUtils.readString(dI));
	}

	public void write(DataOutput dO) throws IOException {
		WritableUtils.writeString(dO, Float.toString(avg));
		WritableUtils.writeString(dO, Float.toString(min));
		WritableUtils.writeString(dO, Float.toString(max));
		WritableUtils.writeString(dO, Float.toString(count));
		WritableUtils.writeString(dO, Float.toString(sum));
		WritableUtils.writeString(dO, Float.toString(stdDev));
	}

	public float getAvg() {
		return avg;
	}

	public void setAvg(float avg) {
		this.avg = avg;
	}

	public float getMin() {
		return min;
	}

	public void setMin(float min) {
		this.min = min;
	}

	public float getMax() {
		return max;
	}

	public void setMax(float max) {
		this.max = max;
	}

	public float getCount() {
		return count;
	}

	public void setCount(float count) {
		this.count = count;
	}
	
	public long getSum() {
		return sum;
	}

	public void setSum(long sum) {
		this.sum = sum;
	}

	public float getStdDev() {
		return stdDev;
	}

	public void setStdDev(float stdDev) {
		this.stdDev = stdDev;
	}

	@Override
	public String toString() {	
		return "min:" + this.min + "|max:" + this.max + "|avg:" + this.avg ; 
	}	

}
