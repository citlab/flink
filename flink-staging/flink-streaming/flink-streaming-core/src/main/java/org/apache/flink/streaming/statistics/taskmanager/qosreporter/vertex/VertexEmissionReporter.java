/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.statistics.taskmanager.qosreporter.vertex;

import org.apache.flink.streaming.statistics.taskmanager.qosmodel.QosReporterID;
import org.apache.flink.streaming.statistics.taskmanager.qosreporter.QosReportForwarderThread;

public class VertexEmissionReporter extends AbstractVertexQosReporter {

	public VertexEmissionReporter(QosReportForwarderThread reportForwarder,
			QosReporterID.Vertex reporterID, int runtimeOutputGateIndex,
			OutputGateEmitStatistics emitCounter) {

		super(reportForwarder, reporterID,
				new ReportTimer(reportForwarder.getAggregationInterval()),
				-1,	runtimeOutputGateIndex, null, emitCounter);
	}

	@Override
	public void recordReceived(int runtimeInputGateIndex) {
		throw new RuntimeException(
				"Method should never be invoked. This is bug.");
	}

	@Override
	public void tryingToReadRecord(int runtimeInputGateIndex) {
		throw new RuntimeException(
				"Method should never be invoked. This is bug.");
	}

	@Override
	public void recordEmitted(int runtimeOutputGateIndex) {
		if (getReportTimer().reportIsDue()) {
			long now = System.currentTimeMillis();
			sendReport(now, null);
		}
	}
}
