package com.adbms.springmvc.pojo;

public class Carrier {
	private String cc;
	private Float minFare;
	private Float maxFare;
	private Float avgFare;
	private String time;
	private Long sumFare;
	
	public String getCc() {
		return cc;
	}
	public void setCc(String cc) {
		this.cc = cc;
	}
	public Float getMinFare() {
		return minFare;
	}
	public void setMinFare(Float minFare) {
		this.minFare = minFare;
	}
	public Float getMaxFare() {
		return maxFare;
	}
	public void setMaxFare(Float maxFare) {
		this.maxFare = maxFare;
	}
	public Float getAvgFare() {
		return avgFare;
	}
	public void setAvgFare(Float avgFare) {
		this.avgFare = avgFare;
	}
	public String getTime() {
		return time;
	}
	public void setTime(String time) {
		this.time = time;
	}
	public Long getSumFare() {
		return sumFare;
	}
	public void setSumFare(Long sumFare) {
		this.sumFare = sumFare;
	}
	
	
	
	
}
