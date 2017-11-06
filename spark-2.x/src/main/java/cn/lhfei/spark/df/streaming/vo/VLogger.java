/*
 * Copyright 2010-2011 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.lhfei.spark.df.streaming.vo;

import cn.lhfei.spark.df.streaming.vlog.VType;
import org.apache.commons.net.ntp.TimeStamp;

import java.io.Serializable;
import java.text.SimpleDateFormat;

/**
 * @version 1.0.0
 *
 * @author Hefei Li
 *
 * @since Aug 15, 2016
 */
public class VLogger implements Serializable {
	static SimpleDateFormat SF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	static final String NORMAL_DATE = "1976-01-01";
	
	public VLogger() {}
	
	public VLogger(String[] items){
		if (items.length == 24) {
			TimeStamp ts = new TimeStamp(Long.parseLong(items[11]));
			
			err = items[15];
			if (err != null && err.startsWith("301030_")) {
				err = "301030_X";
			}
			
			this.setErr(err);
			this.setIp(items[2]);
			this.setRef(items[3]);
			this.setSid(items[4]);
			this.setUid(items[5]);
			this.setLoc(items[8]);
			this.setCat(VType.getVType(items[21], items[7], items[20]));
			
			try {
				this.setTm(SF.format(ts));
			} catch (Exception e) {
				this.setTm(NORMAL_DATE);
			}
			
			this.setUrl(items[12]);
			this.setDur(items[14]);
			this.setBt(items[16]);
			this.setBl(items[17]);
			this.setLt(items[18]);
			this.setVid(items[20]);
			this.setPtype(items[21]);
			this.setCdnId(items[22]);
			this.setNetname(items[23]);
		}		
	}
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -2649470243994866285L;
	public String getErr() {
		return err;
	}
	public void setErr(String err) {
		this.err = err;
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public String getRef() {
		return ref;
	}
	public void setRef(String ref) {
		this.ref = ref;
	}
	public String getSid() {
		return sid;
	}
	public void setSid(String sid) {
		this.sid = sid;
	}
	public String getUid() {
		return uid;
	}
	public void setUid(String uid) {
		this.uid = uid;
	}
	public String getFrom() {
		return from;
	}
	public void setFrom(String from) {
		this.from = from;
	}
	public String getLoc() {
		return loc;
	}
	public void setLoc(String loc) {
		this.loc = loc;
	}
	public String getCat() {
		return cat;
	}
	public void setCat(String cat) {
		this.cat = cat;
	}
	public String getTm() {
		return tm;
	}
	public void setTm(String tm) {
		this.tm = tm;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getDur() {
		return dur;
	}
	public void setDur(String dur) {
		this.dur = dur;
	}
	public String getBt() {
		return bt;
	}
	public void setBt(String bt) {
		this.bt = bt;
	}
	public String getBl() {
		return bl;
	}
	public void setBl(String bl) {
		this.bl = bl;
	}
	public String getLt() {
		return lt;
	}
	public void setLt(String lt) {
		this.lt = lt;
	}
	public String getVid() {
		return vid;
	}
	public void setVid(String vid) {
		this.vid = vid;
	}
	public String getPtype() {
		return ptype;
	}
	public void setPtype(String ptype) {
		this.ptype = ptype;
	}
	public String getCdnId() {
		return cdnId;
	}
	public void setCdnId(String cdnId) {
		this.cdnId = cdnId;
	}
	public String getNetname() {
		return netname;
	}
	public void setNetname(String netname) {
		this.netname = netname;
	}
	public String getTr() {
		return tr;
	}
	public void setTr(String tr) {
		this.tr = tr;
	}
	
	
	public void adapter(String[] items) {
		if (items.length == 24) {
			TimeStamp ts = new TimeStamp(Long.parseLong(items[11]));
			
			err = items[15];
			if (err != null && err.startsWith("301030_")) {
				err = "301030_X";
			}
			
			this.setErr(err);
			this.setIp(items[2]);
			this.setRef(items[3]);
			this.setSid(items[4]);
			this.setUid(items[5]);
			this.setLoc(items[8]);
			this.setCat(VType.getVType(items[21], items[7], items[20]));
			
			try {
				this.setTm(SF.format(ts));
			} catch (Exception e) {
				this.setTm(NORMAL_DATE);
			}
			
			this.setUrl(items[12]);
			this.setDur(items[14]);
			this.setBt(items[16]);
			this.setBl(items[17]);
			this.setLt(items[18]);
			this.setVid(items[20]);
			this.setPtype(items[21]);
			this.setCdnId(items[22]);
			this.setNetname(items[23]);
		}		
	}
	
	private String err;
	private String ip;
	private String ref;
	private String sid;
	private String uid;
	private String from;
	private String loc;
	private String cat;
	private String tm;
	private String url;
	private String dur;
	private String bt;
	private String bl;
	private String lt;
	private String vid;
	private String ptype;
	private String cdnId;
	private String netname;
	private String tr;

}
