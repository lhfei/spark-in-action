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


/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since  Jul 31, 2015
 */
public class VideologFilterTest {

	public static void main(String[] args) {
		String origin = "0366d09a-9502-433b-b419-e782b372aed9	#	59.61.117.75	http://v.ifeng.com/documentary/society/201506/0366d09a-9502-433b-b419-e782b372aed9.shtml	#	1419229448395_odxl05931	#	vdocumentary	#	#	#	1435809517436	http://ips.ifeng.com/wideo.ifeng.com/documentary/2015/06/30/3413547-102-008-1557_4.mp4?time=1435808829745	#	298.44	402000	0	0	510	#	vDocPlayer_vp2.0.31	0029	#	#";
	
		VideologPair pair = VideologFilter.filte(origin, "2015-07-30", "0959");
		
		
		System.out.println(pair.getKey());
		System.out.println(pair.getValue());
	}

}
