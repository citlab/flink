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
import org.apache.flink.streaming.statistics.taskmanager.qosreporter.sampling.Sample;

public class VertexConsumptionReporter extends AbstractVertexQosReporter {
	
	private final InputGateInterReadTimeSampler igInterReadSampler;
	
	public VertexConsumptionReporter(QosReportForwarderThread reportForwarder,
			QosReporterID.Vertex reporterID, int runtimeInputGateIndex,
			InputGateReceiveCounter igReceiveCounter) {

		super(reportForwarder, reporterID,
				new ReportTimer(reportForwarder.getAggregationInterval()),
				runtimeInputGateIndex, -1, igReceiveCounter, null);
		
		igInterReadSampler = new InputGateInterReadTimeSampler(
				reportForwarder.getSamplingProbability() / 100.0);
	}

	@Override
	public void recordReceived(int runtimeInputGateIndex) {
		if (runtimeInputGateIndex == getRuntimeInputGateIndex()) {
			igInterReadSampler.recordReceivedOnIg();
			
			if (igInterReadSampler.hasSample() && canSendReport()) {
				long now = System.currentTimeMillis();
				Sample readReadTime = igInterReadSampler.drawSampleAndReset(now).rescale(0.001);
				sendReport(now, readReadTime);
			}
		}
	}

	@Override
	public void tryingToReadRecord(int runtimeInputGateIndex) {
		igInterReadSampler.tryingToReadRecordFromAnyIg();
	}

	@Override
	public void recordEmitted(int runtimeOutputGateIndex) {
		throw new RuntimeException("Method not implemented.");
	}
}
