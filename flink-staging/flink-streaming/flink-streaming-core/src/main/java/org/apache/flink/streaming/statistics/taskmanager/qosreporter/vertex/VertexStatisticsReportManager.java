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

import org.apache.flink.streaming.statistics.SamplingStrategy;
import org.apache.flink.streaming.statistics.message.qosreport.VertexStatistics;
import org.apache.flink.streaming.statistics.taskmanager.qosmodel.QosReporterID;
import org.apache.flink.streaming.statistics.taskmanager.qosreporter.InputGateReporterManager;
import org.apache.flink.streaming.statistics.taskmanager.qosreporter.OutputGateReporterManager;
import org.apache.flink.streaming.statistics.taskmanager.qosreporter.QosReportForwarderThread;
import org.apache.flink.streaming.statistics.taskmanager.qosreporter.StreamTaskQosCoordinator;
import org.apache.flink.streaming.statistics.types.TimeStampedRecord;
import org.apache.flink.streaming.statistics.util.StreamUtil;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Handles the measurement and reporting of latencies and record
 * consumption/emission rates for a particular vertex. Such a latency is defined
 * as the timespan between record receptions and emits on a particular
 * input/output gate combination of the vertex. Thus one vertex may have
 * multiple associated latencies, one for each input/output gate combination.
 * Which gate combination is measured and reported on must be configured by
 * calling {@link #addReporter(int, int, QosReporterID.Vertex, SamplingStrategy)}.
 * <p/>
 * An {@link VertexStatistics} record per configured input/output gate
 * combination will be handed to the provided {@link QosReportForwarderThread}
 * approximately once per aggregation interval (see
 * {@link StreamTaskQosCoordinator}). "Approximately" because if no records have
 * been received/emitted, nothing will be reported.
 * 
 * @author Bjoern Lohrmann
 */
public class VertexStatisticsReportManager {

	// private static final Log LOG =
	// LogFactory.getLog(TaskLatencyReporter.class);

	private final QosReportForwarderThread reportForwarder;

	/**
	 * For each input gate of the task for whose channels latency reporting is
	 * required, this list contains a InputGateReporterManager. A
	 * InputGateReporterManager keeps track of and reports on the latencies for
	 * all of the input gate's channels. This is a sparse list (may contain
	 * nulls), indexed by the runtime gate's own indices.
	 */
	private AtomicReferenceArray<InputGateReporterManager> inputGateReporters;

	/**
	 * For each output gate of the task for whose output channels QoS statistics
	 * are required (throughput, output buffer lifetime, ...), this list
	 * contains a OutputGateReporterManager. Each OutputGateReporterManager
	 * keeps track of and reports on Qos statistics all of the output gate's
	 * channels and also attaches tags to records sent via its channels. This is
	 * a sparse list (may contain nulls), indexed by the runtime gate's own
	 * indices.
	 */
	private AtomicReferenceArray<OutputGateReporterManager> outputGateReporters;

	private final ConcurrentHashMap<QosReporterID, VertexQosReporter> reporters;
	private final AtomicReferenceArray<VertexQosReporter[]> reportersByInputGate;
	private final AtomicReferenceArray<VertexQosReporter[]> reportersByOutputGate;

	public VertexStatisticsReportManager(QosReportForwarderThread qosReporter,
			int noOfInputGates, int noOfOutputGates) {

		this.reportForwarder = qosReporter;

		this.inputGateReporters = new AtomicReferenceArray<InputGateReporterManager>(
				noOfInputGates);
		this.outputGateReporters = new AtomicReferenceArray<OutputGateReporterManager>(
				noOfOutputGates);

		this.reportersByInputGate = StreamUtil
				.createAtomicReferenceArrayOfEmptyArrays(
						VertexQosReporter.class, noOfInputGates);
		this.reportersByOutputGate = StreamUtil
				.createAtomicReferenceArrayOfEmptyArrays(
						VertexQosReporter.class, noOfOutputGates);

		this.reporters = new ConcurrentHashMap<QosReporterID, VertexQosReporter>();
	}

	public void recordReceived(int runtimeInputGateIndex) {
		InputGateReporterManager igCounter = inputGateReporters.get(runtimeInputGateIndex);

		if (igCounter != null) {
			igCounter.countRecord();
		}

		for (VertexQosReporter reporter : this.reportersByInputGate
				.get(runtimeInputGateIndex)) {
			reporter.recordReceived(runtimeInputGateIndex);
		}
	}

	public void tryingToReadRecord(int runtimeInputGateIndex) {
		for (VertexQosReporter reporter : this.reportersByInputGate
				.get(runtimeInputGateIndex)) {
			reporter.tryingToReadRecord(runtimeInputGateIndex);
		}
	}

	public void recordEmitted(int gateIndex, int channelIndex, TimeStampedRecord record) {
		OutputGateReporterManager reporter = outputGateReporters.get(gateIndex);
		if (reporter != null) {
			reporter.countRecord();
			if (reporter.isReporter()) {
				reporter.recordEmitted(channelIndex, record);
			}
		}
	}

	public void outputBufferSent(int gateIndex, int channelIndex, long currentAmountTransmitted) {
		OutputGateReporterManager outputGateReporter = outputGateReporters.get(gateIndex);
		if (outputGateReporter != null && outputGateReporter.isReporter()) {
			outputGateReporter.outputBufferSent(channelIndex, currentAmountTransmitted);
		}
	}

	public void outputBufferAllocated(int gateIndex, int channelIndex) {
		OutputGateReporterManager outputGateReporter = outputGateReporters.get(gateIndex);
		if (outputGateReporter != null) {
			outputGateReporter.outputBufferAllocated(channelIndex);
		}
	}

	public void reportLatenciesIfNecessary(int gateIndex, int inputChannel, long timestamp) {
		InputGateReporterManager inputGateReporter = inputGateReporters.get(gateIndex);
		if (inputGateReporter != null) {
			inputGateReporter.reportLatencyIfNecessary(inputChannel, timestamp);
		}
	}

	public boolean containsReporter(QosReporterID.Vertex reporterID) {
		return this.reporters.containsKey(reporterID);
	}

	public synchronized void addReporter(int runtimeInputGateIndex,
			int runtimeOutputGateIndex, QosReporterID.Vertex reporterID,
			SamplingStrategy samplingStrategy) {

		if (this.reporters.containsKey(reporterID)) {
			return;
		}

		if (!reporterID.isDummy()) {
			inputGateReporters.compareAndSet(runtimeInputGateIndex, null,
					new InputGateReporterManager());
			outputGateReporters.compareAndSet(runtimeOutputGateIndex, null,
					new OutputGateReporterManager());

			switch (samplingStrategy) {
			case READ_WRITE:
				addReadWriteReporter(runtimeInputGateIndex,
						runtimeOutputGateIndex, reporterID);
				break;

			case READ_READ:
				addReadReadReporter(runtimeInputGateIndex,
						runtimeOutputGateIndex, reporterID);
				break;
			default:
				throw new IllegalArgumentException(
						"Unsupported sampling strategy: " + samplingStrategy);
			}

		} else if (runtimeInputGateIndex != -1) {
			inputGateReporters.compareAndSet(runtimeInputGateIndex, null,
					new InputGateReporterManager());
			addVertexConsumptionReporter(runtimeInputGateIndex, reporterID);

		} else if (runtimeOutputGateIndex != -1) {
			outputGateReporters.compareAndSet(runtimeOutputGateIndex, null,
					new OutputGateReporterManager());
			addVertexEmissionReporter(runtimeOutputGateIndex, reporterID);
		}
	}

	private void addVertexEmissionReporter(int runtimeOutputGateIndex,
			QosReporterID.Vertex reporterID) {
		VertexEmissionReporter reporter = new VertexEmissionReporter(
				reportForwarder, reporterID, runtimeOutputGateIndex,
				outputGateReporters.get(runtimeOutputGateIndex));

		addToReporterArray(reportersByOutputGate, runtimeOutputGateIndex,
				reporter);
		this.reporters.put(reporterID, reporter);
	}

	private void addVertexConsumptionReporter(int runtimeInputGateIndex,
			QosReporterID.Vertex reporterID) {
		VertexConsumptionReporter reporter = new VertexConsumptionReporter(
				reportForwarder, reporterID, runtimeInputGateIndex,
				inputGateReporters.get(runtimeInputGateIndex));

		addToReporterArray(reportersByInputGate, runtimeInputGateIndex,
				reporter);
		this.reporters.put(reporterID, reporter);
	}

	public void addReadReadReporter(int runtimeInputGateIndex,
			int runtimeOutputGateIndex, QosReporterID.Vertex reporterID) {
		
		// search for ReadReadVertexQosReporterGroup
		ReadReadVertexQosReporterGroup groupReporter = null;
		for (VertexQosReporter vertexQosReporter : reportersByInputGate.get(runtimeInputGateIndex)) {
			if (vertexQosReporter instanceof ReadReadVertexQosReporterGroup
					&& vertexQosReporter.getRuntimeInputGateIndex() == runtimeInputGateIndex) {

				groupReporter = (ReadReadVertexQosReporterGroup) vertexQosReporter;
				break;
			}
		}
		
		// create a ReadReadVertexQosReporterGroup if none found for the given
		// runtimeInputGateIndex
		if (groupReporter == null) {
			groupReporter = new ReadReadVertexQosReporterGroup(reportForwarder, runtimeInputGateIndex,
					inputGateReporters.get(runtimeInputGateIndex));
			// READ_READ reporters needs to keep track of all input gates
			for (int i = 0; i < this.reportersByInputGate.length(); i++) {
				addToReporterArray(reportersByInputGate, i, groupReporter);
			}
		}

		ReadReadReporter reporter = new ReadReadReporter(reportForwarder,
				reporterID, groupReporter.getReportTimer(), 
				runtimeInputGateIndex, runtimeOutputGateIndex,
				inputGateReporters.get(runtimeInputGateIndex),
				outputGateReporters.get(runtimeOutputGateIndex));

		groupReporter.addReporter(reporter);
		this.reporters.put(reporterID, reporter);
	}

	public void addReadWriteReporter(int runtimeInputGateIndex,
			int runtimeOutputGateIndex, QosReporterID.Vertex reporterID) {

		VertexQosReporter reporter = new ReadWriteReporter(reportForwarder,
				reporterID, runtimeInputGateIndex, runtimeOutputGateIndex,
				inputGateReporters.get(runtimeInputGateIndex),
				outputGateReporters.get(runtimeOutputGateIndex));

		// for the READ_WRITE strategy the reporter needs to keep track of the
		// events on
		// all input gates
		for (int igIndex = 0; igIndex < this.reportersByInputGate.length(); igIndex++) {
			addToReporterArray(reportersByInputGate, igIndex, reporter);
		}
		// for the READ_WRITE strategy the reporter needs to keep track of the
		// events on
		// all output gates, too
		for (int ogIndex = 0; ogIndex < this.reportersByOutputGate.length(); ogIndex++) {
			addToReporterArray(reportersByOutputGate, ogIndex, reporter);
		}
		this.reporters.put(reporterID, reporter);
	}

	private void addToReporterArray(
			AtomicReferenceArray<VertexQosReporter[]> reporterArray,
			int gateIndex, VertexQosReporter reporter) {

		reporterArray.set(gateIndex,
				StreamUtil.appendToArrayAt(reporterArray.get(gateIndex),
						VertexQosReporter.class, reporter));
	}

	public void inputBufferConsumed(int inputGateIndex, int channelIndex,
			long bufferInterarrivalTimeNanos, int recordsReadFromBuffer) {

		for (VertexQosReporter reporter : this.reportersByInputGate
				.get(inputGateIndex)) {
			reporter.inputBufferConsumed(inputGateIndex, channelIndex,
					bufferInterarrivalTimeNanos, recordsReadFromBuffer);
		}
	}

	public InputGateReporterManager addInputGateReporter(int gateIndex, int channelIndex, int numberOfInputChannels,
			QosReporterID.Edge reporterID) {
		inputGateReporters.compareAndSet(gateIndex, null, new InputGateReporterManager());
		InputGateReporterManager inputGateReporter = inputGateReporters.get(gateIndex);
		inputGateReporter.initReporter(reportForwarder, numberOfInputChannels);
		inputGateReporter.addEdgeQosReporterConfig(channelIndex, reporterID);

		return inputGateReporter;
	}

	public OutputGateReporterManager addOutputGateReporter(int gateIndex, int channelIndex, int numberOfOutputChannels,
			QosReporterID.Edge reporterID) {
		outputGateReporters.compareAndSet(gateIndex, null, new OutputGateReporterManager());
		OutputGateReporterManager outputGateReporter = outputGateReporters.get(gateIndex);
		outputGateReporter.initReporter(reportForwarder, numberOfOutputChannels);
		outputGateReporter.addEdgeQosReporterConfig(channelIndex, reporterID);

		return outputGateReporter;
	}
}
