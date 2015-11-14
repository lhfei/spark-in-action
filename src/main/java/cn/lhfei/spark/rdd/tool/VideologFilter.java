/*
 * Copyright 2010-2014 the original author or authors.
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
package cn.lhfei.spark.rdd.tool;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Set;
import java.util.TreeSet;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since  May 25, 2015
 */
public class VideologFilter {
	
	private static final String INVALID_DATE_STR = "####-##-##";

	/**
	 * 
	 */
	public static Set<String> VALID_ERR_CODE = new TreeSet<String>(
			Arrays.asList("208000", "303000", 
					"301010", "301020", "301030",
					"304001", "304002", "304003", "304004", 
					"601000", "602000"));
	
	/**
	 * <ol><tt>播放器版本统计对象</tt></br>
	 * 	<li>ExtPlayer_V5.3.3 站外播放器
	 * 	<li>vNsPlayer_nsvp1.0.18
	 */
	public static Set<String> VALID_PLAYER_VERSION = new TreeSet<String>(
			Arrays.asList("VZHPlayer_zhvp1.0.16", "vNsPlayer_nsvp1.0.18", "ExtPlayer_V5.3.3"));
	
	public static String formatDate(String dateStr) {
		try {
			if(null != dateStr && dateStr.trim().length() > 0){
				SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
				long tm = Long.parseLong(dateStr);
				Date date = new Date(tm);
				
				dateStr = sf.format(date);
			}else{
				dateStr = INVALID_DATE_STR;
			}
		} catch (NumberFormatException e) {
			dateStr = INVALID_DATE_STR;
		}
		return dateStr;
	}
	
	
	public static VideologPair filte(String origin, String ds, String tm){
		VideologPair pair = null;
		try {
			if(null != origin && origin.length() > 0){
				String separator = "\t";
				String[] items = origin.toString().split(separator);
				
				String trString = "";
				String hour = tm.substring(0,2);
				String minutes = tm.substring(2, 4);
				int tr = ((Integer.parseInt(minutes) / 10) + 1) * 10;	//time range, for example: 15:03 will be pass into range 15:10
				
				if(tr == 60) {
					int hourInt = Integer.parseInt(hour) + 1;
					if(hourInt < 10){
						hour = "0" +hourInt;
					}else {
						hour = "" +hour;
					}
					trString = hourInt + ":" +"00";
 				}else {
 					trString = hour + ":" +tr;
 				}
				
				String timestamp = ds +" "+ hour + ':' +minutes; 
				
				String err = "";
				if(items.length == 24){
					err = items[15];
					if(err != null && err.startsWith("301030_")){
						err = "301030_X";
					};
					
					StringBuilder sb = new StringBuilder();
					pair = new VideologPair(err);		//err
					pair.setIp(items[2]);
					
					sb.append(items[2]);	//ip
					sb.append(separator);
					
					sb.append(items[3]);	//ref
					sb.append(separator);
					
					sb.append(items[4]);	//sid
					sb.append(separator);
					
					sb.append(items[5]);	//uid
					sb.append(separator);
					
					/*sb.append(items[7]);	//from
					sb.append(separator);*/
					
					sb.append(items[8]);	//loc
					sb.append(separator);
					
					sb.append(VType.getVType(items[21], items[7], items[20]));	//cat
					sb.append(separator);
					
					sb.append(timestamp);	//tm
					sb.append(separator);
					
					sb.append(items[12]);	//url
					sb.append(separator);
					
					sb.append(items[14]);	//dur
					sb.append(separator);
					
					sb.append(items[16]);	//bt
					sb.append(separator);
					
					sb.append(items[17]);	//bl
					sb.append(separator);
					
					sb.append(items[18]);	//lt
					sb.append(separator);
					
					sb.append(items[20]);	// vid
					sb.append(separator);
					
					sb.append(items[21]);	// ptype
					sb.append(separator);
					
					sb.append(items[22]);	//cdnId
					sb.append(separator);
					
					sb.append(items[23]);	//netname
					sb.append(separator);
					
					sb.append(trString);	//tr
					
					pair.setValue(sb.toString());
					
				}
			}
		} catch (Exception e) {
		}
		
		return pair;
	}
	
}

/**
 * 播放器统计对象<p>
 * <ol><tt>播放器版本统计对象</tt></br>
 * 	<li>ExtPlayer_V5.3.3 站外播放器
 * 	<li>vNsPlayer_nsvp1.0.18
 * @author Hefei Li
 *
 */
class VPlayer  {
	
	/**
	 *  牛市播放器
	 */
	public static final String NS_Major = "vNsPlayer_nsvp1.0.18";
	
	/**
	 *  站外播放器
	 */
	public static final String External_Major = "ExtPlayer_V5.3.3";
	
	public static boolean isValid(String player) {
		if(player != null){
			if(player.equals(NS_Major) || player.equals(External_Major)){
				return true;
			}else{
				return false;
			}
		}else{
			return false;
		}
	}
}
