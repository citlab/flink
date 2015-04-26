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

public abstract class AbstractVertexQosReporter implements VertexQosReporter {
	
	private final QosReporterID.Vertex reporterID;

	private final QosReportForwarderThread reportForwarder;
	
	private final ReportTimer reportTimer;

	private long igReceiveCounterAtLastReport;
	private final InputGateReceiveCounter igReceiveCounter;
	private final InputGateInterArrivalTimeSampler igInterarrivalTimeSampler;

	private long ogEmitCounterAtLastReport;
	private final OutputGateEmitStatistics ogEmitCounter;

	private final int runtimeInputGateIndex;

	private final int runtimeOutputGateIndex;

	public AbstractVertexQosReporter(QosReportForwarderThread reportForwarder,
			QosReporterID.Vertex reporterID, ReportTimer reportTimer,
			int runtimeInputGateIndex,
			int runtimeOutputGateIndex, InputGateReceiveCounter igReceiveCounter,
			OutputGateEmitStatistics emitCounter) {

		this.reportForwarder = reportForwarder;
		this.reporterID = reporterID;
		this.reportTimer = reportTimer;
		
		this.runtimeInputGateIndex = runtimeInputGateIndex;
		this.runtimeOutputGateIndex = runtimeOutputGateIndex;

		if (reporterID.hasInputDataSetID()) {
			this.igReceiveCounterAtLastReport = igReceiveCounter.getRecordsReceived();
			this.igReceiveCounter = igReceiveCounter;
			this.igInterarrivalTimeSampler = new InputGateInterArrivalTimeSampler(
					reportForwarder.getSamplingProbability() / 100.0);
		} else {
			this.igReceiveCounter = null;
			this.igInterarrivalTimeSampler = null;
		}

		if (reporterID.hasOutputDataSetID()) {
			this.ogEmitCounterAtLastReport = emitCounter.getEmitted();
			this.ogEmitCounter = emitCounter;
		} else {
			this.ogEmitCounter = null;
		}
	}

	public QosReporterID.Vertex getReporterID() {
		return reporterID;
	}
	
	public ReportTimer getReportTimer() {
		return this.reportTimer;
	}
	
	public void inputBufferConsumed(int inputGateIndex, int channelIndex,
			long bufferInterarrivalTimeNanos, int recordsReadFromBuffer) {
		
		igInterarrivalTimeSampler.inputBufferConsumed(channelIndex, bufferInterarrivalTimeNanos, recordsReadFromBuffer);
	}
	
	public boolean canSendReport() {
		return igInterarrivalTimeSampler.hasSample() && reportTimer.reportIsDue();
	}
	
	public void sendReport(long now, 
			Sample igInterReadTimeMillis) {
		
		double secsPassed = (now - reportTimer.getTimeOfLastReport()) / 1000.0;
		
		VertexStatistics toSend = null;
		
		if (reporterID.hasInputDataSetID() && reporterID.hasOutputDataSetID()) {
			toSend = new VertexStatistics(reporterID,
					igInterReadTimeMillis,
					getRecordsConsumedPerSec(secsPassed),
					getRecordsEmittedPerSec(secsPassed),
					igInterarrivalTimeSampler.drawSampleAndReset(now).rescale(0.001));

		} else if (reporterID.hasInputDataSetID()) {
			toSend = new VertexStatistics(reporterID,
					igInterReadTimeMillis,
					getRecordsConsumedPerSec(secsPassed),
					igInterarrivalTimeSampler.drawSampleAndReset(now).rescale(0.001));

		} else {
			toSend = new VertexStatistics(reporterID,
					getRecordsEmittedPerSec(secsPassed));
		}

		reportTimer.reset(now);
		reportForwarder.addToNextReport(toSend);
	}
			
	private double getRecordsConsumedPerSec(double secsPassed) {
		double recordsConsumedPerSec = -1;
		if (igReceiveCounter != null) {
			recordsConsumedPerSec = (igReceiveCounter.getRecordsReceived() - igReceiveCounterAtLastReport)
					/ secsPassed;
			igReceiveCounterAtLastReport = igReceiveCounter.getRecordsReceived();
		}
		return recordsConsumedPerSec;
	}
	
	private double getRecordsEmittedPerSec(double secsPassed) {
		double recordEmittedPerSec = -1;
		if (ogEmitCounter != null) {
			recordEmittedPerSec = (ogEmitCounter.getEmitted() - ogEmitCounterAtLastReport)
					/ secsPassed;
			ogEmitCounterAtLastReport = ogEmitCounter.getEmitted();
		}
		return recordEmittedPerSec;
	}

	@Override
	public int getRuntimeInputGateIndex() {
		return runtimeInputGateIndex;
	}

	@Override
	public int getRuntimeOutputGateIndex() {
		return runtimeOutputGateIndex;
	}
}
