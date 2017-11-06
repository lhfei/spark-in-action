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

import org.apache.hive.service.cli.thrift.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;

import java.util.List;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since Oct 22, 2015
 */

public class ThriftSocketClient {

	public static void main(String[] args) {
		try {
			TSocket transport = new TSocket("10.154.29.150", 10002);
			transport.setTimeout(999999999);
			TBinaryProtocol protocol = new TBinaryProtocol(transport);
			TCLIService.Client client = new TCLIService.Client(protocol);

			transport.open();

			TOpenSessionReq openReq = new TOpenSessionReq();
			TOpenSessionResp openResp = client.OpenSession(openReq);
			TSessionHandle sessHandle = openResp.getSessionHandle();
			TExecuteStatementReq execReq = new TExecuteStatementReq(sessHandle, "SELECT * FROM temp.uuid_count limit 10;");
			TExecuteStatementResp execResp = client.ExecuteStatement(execReq);
			TOperationHandle stmtHandle = execResp.getOperationHandle();
			TFetchResultsReq fetchReq = new TFetchResultsReq(stmtHandle, TFetchOrientation.FETCH_FIRST, 1);
			TFetchResultsResp resultsResp = client.FetchResults(fetchReq);

			TRowSet resultsSet = resultsResp.getResults();
			List<TRow> resultRows = resultsSet.getRows();
			for (TRow resultRow : resultRows) {
				resultRow.toString();
			}

			TCloseOperationReq closeReq = new TCloseOperationReq();
			closeReq.setOperationHandle(stmtHandle);
			client.CloseOperation(closeReq);
			TCloseSessionReq closeConnectionReq = new TCloseSessionReq(sessHandle);
			client.CloseSession(closeConnectionReq);

			transport.close();
		} catch (TException e) {
			e.printStackTrace();
		}
	}

}
