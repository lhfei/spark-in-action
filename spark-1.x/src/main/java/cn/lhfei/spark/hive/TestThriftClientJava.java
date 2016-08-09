package cn.lhfei.spark.hive;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;

public class TestThriftClientJava {
	public void testThriftClient() throws TException {

		final TSocket tSocket = new TSocket("10.154.29.150", 10002);
		final TProtocol protocol = new TBinaryProtocol(tSocket);
		/*final HiveClient client = new HiveClient(protocol);
		tSocket.open();
		client.execute("show tables");
		final List<String> results = client.fetchAll();
		for (String result : results) {
			System.out.println(result);
		}
		tSocket.close();*/
	}
}
