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

package cn.lhfei.spark.hive;

import java.util.List;

import org.apache.hadoop.hive.service.HiveServerException;
import org.apache.hadoop.hive.service.ThriftHive;
import org.apache.hadoop.hive.service.ThriftHive.Client;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since Oct 22, 2015
 */

public class HiveThriftClient {
	private static final Logger log = LoggerFactory.getLogger(HiveThriftClient.class);

	public static void main(String[] args) {
		try {
			TSocket transport = new TSocket("10.58.62.142", 10003);
			transport.setTimeout(999999999);
			TBinaryProtocol protocol = new TBinaryProtocol(transport);
			
			Client client = new ThriftHive.Client(protocol); 
			
			transport.open();
			
			String hql = "select c.dt,sum(c.num),count(distinct c.imei) from (select count(1) as num,imei,dt from data_sum.sum_sdk_phone_event_day where dt='20151022' and app_name='Wallpaper' and widget_id='YL' and event_id='expose' group by dt,imei union all select count(1) as num,props['imei'] as imei,dt from data_sum.sum_sdk_phone_event_day where dt=20151022  and app_name='Wallpaper' and widget_id='YL' and event_id='expose' and imei='-' group by dt,props['imei']) c join (select a.dt,a.imei from (select dt,imei,cast(from_unixtime(cast(substr(activation_halfhour_time,0,10) as bigint),'yyyyMMdd') as string) time from data_sum.sum_phone_source_day where dt='20151022') a where a.time<=a.dt group by a.dt,a.imei) d on c.imei=d.imei where c.dt=d.dt group by c.dt";
			client.execute(hql);

			List<String> result  = client.fetchAll();
			
			for(String row : result){
				log.info(row);
			}
			
			/*client.execute("select count(*) from data_sum.sum_sdk_phone_app_day where dt='20151022'");
			String length = client.fetchOne();
			
			log.info("Count: [{}]", length);*/
		
			transport.close();
		
		} catch (TTransportException e) {
			e.printStackTrace();
		} catch (HiveServerException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		}
		
		

	}

}
