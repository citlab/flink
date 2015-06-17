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

package org.apache.flink.streaming.statistics.taskmanager.qosreporter;

import org.apache.flink.streaming.statistics.message.qosreport.EdgeStatistics;
import org.apache.flink.streaming.statistics.taskmanager.qosmodel.QosReporterID;
import org.apache.flink.streaming.statistics.taskmanager.qosreporter.edge.OutputBufferLifetimeSampler;
import org.apache.flink.streaming.statistics.taskmanager.qosreporter.sampling.BernoulliSampleDesign;
import org.apache.flink.streaming.statistics.taskmanager.qosreporter.vertex.CountingGateReporter;
import org.apache.flink.streaming.statistics.taskmanager.qosreporter.vertex.ReportTimer;
import org.apache.flink.streaming.statistics.types.TimeStampedRecord;

import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A instance of this class keeps track of and reports on the Qos statistics of
 * an output gate's output channels.
 * 
 * This class is thread-safe.
 * 
 * An {@link EdgeStatistics} record per output channel will be handed to the
 * provided {@link QosReportForwarderThread} approximately once per aggregation
 * interval (see {@link StreamTaskQosCoordinator}). "Approximately" because if no
 * records have been received/emitted, nothing will be reported.
 * 
 * 
 * @author Bjoern Lohrmann
 */
public class OutputGateReporterManager extends CountingGateReporter {

	/**
	 * No need for a thread-safe set because it is only accessed in synchronized
	 * methods.
	 */
	private HashSet<QosReporterID> reporters;

	/**
	 * Maps from an output channels index in the runtime gate to the statistics
	 * reporter. This needs to be threadsafe because statistics collection may
	 * already be running while OutputChannelChannelStatisticsReporters are
	 * being added.
	 */
	private CopyOnWriteArrayList<OutputChannelChannelStatisticsReporter> reportersByChannelIndexInRuntimeGate;

	private QosReportForwarderThread reportForwarder;

	private ReportTimer reportTimer;

	public class OutputChannelChannelStatisticsReporter {

		private QosReporterID.Edge reporterID;

		private int channelIndexInRuntimeGate;

		private long timeOfLastReport;

		private long amountTransmittedAtLastReport;

		private long currentAmountTransmitted;

		private int recordsEmittedSinceLastReport;

		private int outputBuffersSentSinceLastReport;

		private int recordsSinceLastTag;

		public final BernoulliSampleDesign recordTaggingSampleDesign;

		private final OutputBufferLifetimeSampler outputBufferLifetimeSampler;

		public OutputChannelChannelStatisticsReporter(
				QosReporterID.Edge reporterID, int channelIndexInRuntimeGate) {

			this.reporterID = reporterID;
			this.channelIndexInRuntimeGate = channelIndexInRuntimeGate;
			this.timeOfLastReport = System.currentTimeMillis();
			this.amountTransmittedAtLastReport = 0;
			this.currentAmountTransmitted = 0;
			this.recordsEmittedSinceLastReport = 0;
			this.outputBuffersSentSinceLastReport = 0;
			this.recordsSinceLastTag = 0;
			this.recordTaggingSampleDesign = new BernoulliSampleDesign(
					OutputGateReporterManager.this.reportForwarder.getSamplingProbability() / 100.0);


			this.outputBufferLifetimeSampler = new OutputBufferLifetimeSampler(
							OutputGateReporterManager.this.reportForwarder.getSamplingProbability() / 100.0);
		}

		/**
		 * Returns the reporterID.
		 * 
		 * @return the reporterID
		 */
		public QosReporterID.Edge getReporterID() {
			return this.reporterID;
		}

		public void sendReportIfDue(long now) {
			if (this.reportIsDue(now)) {
				this.sendReport(now);
				this.reset(now);
			}
		}

		private boolean reportIsDue(long now) {
			return this.recordsEmittedSinceLastReport > 0
					&& this.outputBuffersSentSinceLastReport > 0
					&& this.outputBufferLifetimeSampler.hasSample();
		}

		private void reset(long now) {
			this.timeOfLastReport = now;
			this.amountTransmittedAtLastReport = this.currentAmountTransmitted;
			this.recordsEmittedSinceLastReport = 0;
			this.outputBuffersSentSinceLastReport = 0;
			this.recordTaggingSampleDesign.reset();
			this.outputBufferLifetimeSampler.reset();
		}

		private void sendReport(long now) {

			double secsPassed = (now - this.timeOfLastReport) / 1000.0;
			double mbitPerSec = (this.currentAmountTransmitted - this.amountTransmittedAtLastReport)
					* 8 / (1000000.0 * secsPassed);
			double meanOutputBufferLifetime = outputBufferLifetimeSampler.getMeanOutputBufferLifetimeMillis();
			double recordsPerBuffer = (double) this.recordsEmittedSinceLastReport
					/ this.outputBuffersSentSinceLastReport;
			double recordsPerSecond = this.recordsEmittedSinceLastReport
					/ secsPassed;

			EdgeStatistics channelStatsMessage = new EdgeStatistics(
					this.reporterID, mbitPerSec, meanOutputBufferLifetime,
					recordsPerBuffer, recordsPerSecond);

			OutputGateReporterManager.this.reportForwarder
					.addToNextReport(channelStatsMessage);
		}

		public void updateStatsAndTagRecordIfNecessary(TimeStampedRecord record) {
			this.recordsEmittedSinceLastReport++;
			this.recordsSinceLastTag++;

			boolean shouldSample = recordTaggingSampleDesign.shouldSample();
			if (shouldSample) {
				record.setTimestamp(System.currentTimeMillis());
				this.recordsSinceLastTag = 0;
			} else {
				record.setTimestamp(0);
			}
		}

		public void outputBufferSent(long currentAmountTransmitted) {
			this.outputBuffersSentSinceLastReport++;
			this.currentAmountTransmitted = currentAmountTransmitted;
			this.outputBufferLifetimeSampler.outputBufferSent();
		}

		public void outputBufferAllocated() {
			this.outputBufferLifetimeSampler.outputBufferAllocated();
		}
	}

	public OutputGateReporterManager() {
	}

	public OutputGateReporterManager(QosReportForwarderThread qosReporter,
			int noOfOutputChannels) {

		initReporter(qosReporter, noOfOutputChannels);
	}

	public void initReporter(QosReportForwarderThread qosReporter, int noOfOutputChannels) {
		this.reportForwarder = qosReporter;
		this.reporters = new HashSet<QosReporterID>();
		this.reportersByChannelIndexInRuntimeGate = new CopyOnWriteArrayList<OutputChannelChannelStatisticsReporter>();
		Collections.addAll(this.reportersByChannelIndexInRuntimeGate,
				new OutputChannelChannelStatisticsReporter[noOfOutputChannels]);
		setReporter(true);
	}

	public void recordEmitted(int channelIndex, TimeStampedRecord record) {
		OutputChannelChannelStatisticsReporter outputChannelReporter = this.reportersByChannelIndexInRuntimeGate
				.get(channelIndex);

		if (outputChannelReporter != null) {
			outputChannelReporter.updateStatsAndTagRecordIfNecessary(record);
		}
	}

	public void outputBufferSent(int runtimeGateChannelIndex,
			long currentAmountTransmitted) {

		OutputChannelChannelStatisticsReporter reporter = this.reportersByChannelIndexInRuntimeGate
				.get(runtimeGateChannelIndex);

		if (reporter != null) {
			reporter.outputBufferSent(currentAmountTransmitted);
		}

		sendReportsIfDue(System.currentTimeMillis());
	}

	public void outputBufferAllocated(int runtimeGateChannelIndex) {
		OutputChannelChannelStatisticsReporter reporter = this.reportersByChannelIndexInRuntimeGate
						.get(runtimeGateChannelIndex);

		if (reporter != null) {
			reporter.outputBufferAllocated();
		}
	}

	public void sendReportsIfDue(long now) {
		if (reportTimer.reportIsDue()) {
			for (OutputChannelChannelStatisticsReporter reporter : reportersByChannelIndexInRuntimeGate) {
				if (reporter != null) {
					reporter.sendReportIfDue(now);
				}
			}
			reportTimer.reset(now);
		}
	}

	public synchronized boolean containsReporter(QosReporterID.Edge reporterID) {
		return this.reporters.contains(reporterID);
	}

	public synchronized void addEdgeQosReporterConfig(
			int channelIndexInRuntimeGate, QosReporterID.Edge reporterID) {

		if (this.reporters.contains(reporterID)) {
			return;
		}

		OutputChannelChannelStatisticsReporter channelStats = new OutputChannelChannelStatisticsReporter(
				reporterID, channelIndexInRuntimeGate);

		this.reportersByChannelIndexInRuntimeGate.set(
				channelIndexInRuntimeGate, channelStats);
		this.reporters.add(reporterID);
	}
}
