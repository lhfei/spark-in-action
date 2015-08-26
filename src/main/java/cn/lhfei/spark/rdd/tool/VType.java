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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * \u89c6\u9891\u7c7b\u578b </p>
 * 
 * <ul>
 * 	<li>长视频 <tt>[L]</tt></li>
 * 	<li>短视频 <tt>[S]</tt></li>
 * </ul>
 * @since  Jul 31, 2015
 * 
 * @author Hefei Li
 *
 */
public enum VType {
	
	/**
	 * 长视频
	 */
	Long(LongVideo.All.getCode()),
	
	/**
	 * 短视频
	 */
	Short(ShortVideo.All.getCode()),
	
	/**
	 * 直播视频
	 */
	Live(LiveVideo.All.getCode()),
	
	/**
	 * VIP 视频
	 */
	Vip(VipVideo.All.getCode()),
	
	/**
	 * 无效分类
	 */
	Irregularity("999");
	
	
	VType(String code) {
		this.code = code;
	}
	
	/**
	 * 牛市播放器唯一标识
	 */
	public static final String FROM_TYPE_CODE = "niushi";
	
	/**
	 * 外站播放版本号前缀
	 */
	public static final String EXTERNAL_PLAYER_PREFIX_CODE = "ExtPlayer";
	
	/**
	 * 有效的视频分类编码
	 */
	public static final List<String> VALID_VIDEO_TYPE_CODE = new ArrayList<String>(
			Arrays.asList(
				"0017",	// live
				"0018",	// vip
				"0029", "0033",	// 长视频
				"0019", "0020", "0022", "0024", "0025", "0031" // 短视频
			));
	/**
	 * 有效的长视频视频分类编码
	 */
	public static final List<String> VALID_LONG_VIDEO_TYPE_CODE = new ArrayList<String>(
			Arrays.asList("0029", "0033"));
	/**
	 * 有效的短视频分类编码
	 */
	public static final List<String> VALID_SHORT_VIDEO_TYPE_CODE = new ArrayList<String>(
			Arrays.asList("0019", "0020", "0022", "0024", "0025", "0031"));
	/**
	 * 长视频分类<p>
	 * 
	 * <ol>
	 * 	<li>全部  <tt><code>ptype in [&quot;0029&quot;, &quot;0033&quot;] </code></tt></li>
	 * 	<li>纪录片  <tt><code>ptype = &quot;0029&quot; </code></tt></li>
	 * 	<li>其他长视频  <tt><code>ptype = &quot;0033&quot; </code></tt></li>
	 * 	<li></li>
	 * 
	 * @author Hefei Li
	 */
	public enum LongVideo {
		
		/**
		 * 全部  <tt><code>ptype in [&quot;0029&quot;, &quot;0033&quot;] </code></tt></li>
		 */
		
		All("LG"),
		
		/**
		 * 纪录片  <tt><code>ptype = &quot;0029&quot; </code></tt></li>
		 */
		
		Documentary("LG01"),
		
		/**
		 * 其他长视频  <tt><code>ptype = &quot;0033&quot; </code></tt></li>
		 */
		The_Others("LG99");
		
		LongVideo(String code) {
			this.code = code;
		}
		
		public String getCode() {
			return code;
		}
		
		private String code;
	}

	/**
	 * 短视频分类<p>
	 * 
	 * <ol>
	 * 	<li>全部  <tt><code>ptype in [&quot;0019&quot;, &quot;0031&quot; , &quot;0025&quot; , &quot;0020&quot; , &quot;0024&quot;, &quot;0022&quot;] </code></tt></li>
	 * 	<li>牛市  <tt><code>from = &quot;niushi&quot; </code></tt></li>
	 * 	<li>外站播放 <tt><code>vid = &quot;ExtPlayer_V5.3.3&quot; </code></tt></li>
	 * 	<li>
	 * 		其他短长视频  </br> 
	 * 		<tt><code>ptype in [&quot;0019&quot;, &quot;0031&quot; , &quot;0025&quot; , &quot;0020&quot; , &quot;0024&quot;, &quot;0022&quot;]</code></tt> </br>
	 * 		and <tt><code>from != &quot;niushi&quot; </code></tt> </br>
	 * 		and <tt><code>vid != &quot;ExtPlayer_V5.3.3&quot; </code></tt>
	 * 	</li>
	 * </ol>
	 * @author Hefei Li
	 */
	public enum ShortVideo {
		
		All("ST"),
		
		Niushi("ST01"),
		
		External("ST02"),
		
		The_Others("ST99");
		
		ShortVideo(String code) {
			this.code = code;
		}
		
		public String getCode() {
			return code;
		}
		
		private String code;
	}
	
	/**
	 * 
	 * @author Hefei Li
	 *
	 */
	public enum LiveVideo {
		
		All("LV");
		
		LiveVideo(String code) {
			this.code = code;
		}
		
		public String getCode() {
			return code;
		}
		
		private String code;
	}
	
	/**
	 * 
	 * @author Hefei Li
	 *
	 */
	public enum VipVideo {
		
		All("VIP");
		
		VipVideo(String code) {
			this.code = code;
		}
		
		public String getCode() {
			return code;
		}
		
		private String code;
	}
	
	public static String getVType(String ptype, String from, String vid) {
		String typeCode = "";

		if ((null != ptype)) {
			ptype = ptype.trim();

			if (VALID_VIDEO_TYPE_CODE.contains(ptype)) {
				
				if(VALID_LONG_VIDEO_TYPE_CODE.contains(ptype)){// 长视频分类检查
					switch (ptype) {

					case "0029":
						typeCode = LongVideo.Documentary.getCode();
						break;

					case "0033":
						typeCode = LongVideo.The_Others.getCode();
						break;

					default:
						typeCode = Irregularity.getCode();
						break;
					}

				} else if(ptype.equals("0017")) {// 直播视频分类检查
					typeCode = LiveVideo.All.getCode();
					
				} else if(ptype.equals("0018")) {// VIP 视频分类检查
					typeCode = VipVideo.All.getCode();
				}
				else {// 短视频分类检查
					if(from != null && from.equals(FROM_TYPE_CODE)){
						typeCode = ShortVideo.Niushi.getCode();
					}else if(vid != null && vid.trim().startsWith(EXTERNAL_PLAYER_PREFIX_CODE)) {
						typeCode = ShortVideo.External.getCode();
					}else if(from == null && vid == null){
						typeCode = Irregularity.getCode();
					}else {
						typeCode = ShortVideo.The_Others.getCode();
					}
				}
			} else {
				typeCode = Irregularity.getCode();
			}

		} else {
			typeCode = Irregularity.getCode();
		}

		return typeCode;
	}
	
	
	/**
	 * @return the code
	 */
	public String getCode() {
		return code;
	}

	private String code;
}

