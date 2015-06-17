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

import org.apache.flink.streaming.statistics.message.qosreport.VertexStatistics;
import org.apache.flink.streaming.statistics.taskmanager.qosmodel.QosReporterID;
import org.apache.flink.streaming.statistics.taskmanager.qosreporter.QosReportForwarderThread;
import org.apache.flink.streaming.statistics.taskmanager.qosreporter.sampling.Sample;

/**
 * @author Ilya Verbitskiy, Bjoern Lohrmann
 */
public class ReadReadReporter implements VertexQosReporter {

	private final QosReporterID.Vertex reporterID;

	private final QosReportForwarderThread reportForwarder;
	
	private final ReportTimer reportTimer;

	private long emitCounterAtLastReport;
	private final CountingGateReporter outputGateEmitCounter;

	private final int runtimeInputGateIndex;

	private final int runtimeOutputGateIndex;

	public ReadReadReporter(QosReportForwarderThread reportForwarder,
			QosReporterID.Vertex reporterID,
			ReportTimer reportTimer,
			int runtimeInputGateIndex,
			int runtimeOutputGateIndex,
			CountingGateReporter igReceiveCounter,
			CountingGateReporter emitCounter) {

		this.reportForwarder = reportForwarder;
		this.reporterID = reporterID;
		this.reportTimer = reportTimer;
		
		this.runtimeInputGateIndex = runtimeInputGateIndex;
		this.runtimeOutputGateIndex = runtimeOutputGateIndex;

		emitCounterAtLastReport = emitCounter.getRecordsCount();
		this.outputGateEmitCounter = emitCounter;
	}
	
	public void sendReport(long now, 
			Sample vertexLatencyMillis,
			Sample interarrivalTimeMillis,
			double recordsConsumedPerSec) {
		
		double secsPassed = (now - reportTimer.getTimeOfLastReport()) / 1000.0;
		
		VertexStatistics toSend = new VertexStatistics(reporterID,
					vertexLatencyMillis,
					recordsConsumedPerSec,
					getRecordsEmittedPerSec(secsPassed),
					interarrivalTimeMillis);
		reportForwarder.addToNextReport(toSend);
	}

	private double getRecordsEmittedPerSec(double secsPassed) {
		double recordEmittedPerSec = -1;
		if (outputGateEmitCounter != null) {
			recordEmittedPerSec = (outputGateEmitCounter.getRecordsCount() - emitCounterAtLastReport)
					/ secsPassed;
			emitCounterAtLastReport = outputGateEmitCounter.getRecordsCount();
		}
		return recordEmittedPerSec;
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
		throw new RuntimeException(
				"Method should never be invoked. This is bug.");
	}


	@Override
	public void inputBufferConsumed(int inputGateIndex, int channelIndex,
			long bufferInterarrivalTimeNanos, int recordsReadFromBuffer) {
		throw new RuntimeException(
				"Method should never be invoked. This is bug.");		
	}
	
	@Override
	public int getRuntimeInputGateIndex() {
		return runtimeInputGateIndex;
	}


	@Override
	public int getRuntimeOutputGateIndex() {
		return runtimeOutputGateIndex;
	}


	@Override
	public ReportTimer getReportTimer() {
		return reportTimer;
	}
}
