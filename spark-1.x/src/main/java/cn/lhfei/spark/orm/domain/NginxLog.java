package cn.lhfei.spark.orm.domain;

import java.io.Serializable;

public class NginxLog implements Serializable {
	private static final long serialVersionUID = -7784332679142422375L;
	
	
	
	@Override
	public String toString() {
		return "{ ip = " + ip + ", liveTime = " + liveTime + ", Agent = " + agent + "}";
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public long getLiveTime() {
		return liveTime;
	}
	public void setLiveTime(long liveTime) {
		this.liveTime = liveTime;
	}
	public String getAgent() {
		return agent;
	}
	public void setAgent(String agent) {
		this.agent = agent;
	}
	private String ip;
	private long liveTime;
	private String agent;
}
