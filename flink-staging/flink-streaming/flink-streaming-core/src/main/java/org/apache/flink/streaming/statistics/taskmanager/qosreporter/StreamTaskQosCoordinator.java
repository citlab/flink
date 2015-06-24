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

import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.runtime.io.StreamRecordWriter;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.statistics.message.action.EdgeQosReporterConfig;
import org.apache.flink.streaming.statistics.message.action.QosReporterConfig;
import org.apache.flink.streaming.statistics.message.action.VertexQosReporterConfig;
import org.apache.flink.streaming.statistics.taskmanager.qosmodel.QosReporterID;
import org.apache.flink.streaming.statistics.taskmanager.qosreporter.listener.InputGateQosReportingListener;
import org.apache.flink.streaming.statistics.taskmanager.qosreporter.listener.OutputGateQosReportingListener;
import org.apache.flink.streaming.statistics.taskmanager.qosreporter.vertex.VertexStatisticsReportManager;
import org.apache.flink.streaming.statistics.types.TimeStampedRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * An instance of this class implements Qos data reporting for a specific vertex
 * and its ingoing/outgoing edges on a task manager while the vertex actually
 * runs.
 *
 * This class is thread-safe.
 *
 * @author Bjoern Lohrmann, Sascha Wolke
 */
public class StreamTaskQosCoordinator {

	private static final Logger LOG = LoggerFactory.getLogger(StreamTaskQosCoordinator.class);

	private final StreamTask task;

	private final Environment taskEnvironment;

	private final QosReportForwarderThread forwarderThread;

	/**
	 * For each input/output gate combination for which Qos reports are
	 * required, this {@link VertexStatisticsReportManager} creates the reports.
	 */
	private VertexStatisticsReportManager vertexStatisticsManager;

	public StreamTaskQosCoordinator(StreamTask task) {
		this.task = task;
		this.taskEnvironment = task.getEnvironment();
		this.forwarderThread =
			QosReportForwarderThread.getOrCreateForwarderAndRegisterTask(task);
	}

	public void prepareQosReporting() {
		StreamConfig taskConfig = new StreamConfig(this.task.getTaskConfiguration());

		for (QosReporterConfig config : taskConfig.getQosReporterConfigs()) {
			if (config instanceof VertexQosReporterConfig) {
				prepareVertexStatisticsReporters((VertexQosReporterConfig) config);
			} else {
				prepareEdgeStatisticsReporters((EdgeQosReporterConfig) config);
			}
		}
	}

	public void cleanup() {
		this.forwarderThread.unregisterTask(this.task);
	}


	public VertexStatisticsReportManager getOrCreateVertexStatisticsManager() {
		if (this.vertexStatisticsManager == null) {
			this.vertexStatisticsManager = new VertexStatisticsReportManager(
					this.forwarderThread,
					this.taskEnvironment.getAllInputGates().length,
					this.taskEnvironment.getAllWriters().length);
		}
		return vertexStatisticsManager;
	}

//	public synchronized void handleLimitBufferSizeAction(
//			LimitBufferSizeAction limitBufferSizeAction) {
//		TODO
//	}

//	public void handleSetOutputLatencyTargetAction(
//			SetOutputBufferLifetimeTargetAction action) {
//		TODO
//	}

	public void setupInputQosListener(QosReportingReader input) {
		StreamConfig taskConfig = new StreamConfig(this.task.getTaskConfiguration());
		List<QosReporterConfig> qosReporterConfigs = taskConfig.getQosReporterConfigs();

		for (QosReporterConfig config : qosReporterConfigs) {
			if (config instanceof VertexQosReporterConfig) {

				VertexQosReporterConfig vertexConfig = (VertexQosReporterConfig) config;
				VertexStatisticsReportManager vertexStatisticsManager = this.getOrCreateVertexStatisticsManager();
				IntermediateDataSetID inputDataSetID = vertexConfig.getInputDataSetID();
				int inputGateIndex = vertexConfig.getInputGateIndex();

				if (inputGateIndex != -1 && inputDataSetID != null) {
					ensureInputGateListener(input, inputGateIndex);
				}

			} else if (config instanceof EdgeQosReporterConfig) {
				EdgeQosReporterConfig edgeConfig = (EdgeQosReporterConfig) config;
				int inputGateIndex = edgeConfig.getInputGateIndex();

				if (edgeConfig.isTargetTaskConfig()) {
					ensureInputGateListener(input, inputGateIndex);
				}
			}
		}
	}

	public void setupOutputQosListener(StreamRecordWriter<?> writer, int outputIndex) {
		StreamConfig taskConfig = new StreamConfig(task.getTaskConfiguration());
		List<QosReporterConfig> qosReporterConfigs = taskConfig.getQosReporterConfigs();

		for (QosReporterConfig config : qosReporterConfigs) {
			if (config instanceof VertexQosReporterConfig) {

				VertexQosReporterConfig vertexConfig = (VertexQosReporterConfig) config;

				final VertexStatisticsReportManager vertexStatisticsManager = this.getOrCreateVertexStatisticsManager();

				IntermediateDataSetID outputDataSetID = vertexConfig.getOutputDataSetID();
				final int outputGateIndex = vertexConfig.getOutputGateIndex();
				if (outputDataSetID != null && outputIndex == outputGateIndex) {
					ensureOutputGateListener(writer, outputIndex);
				}

			} else if (config instanceof EdgeQosReporterConfig) {
				EdgeQosReporterConfig edgeConfig = (EdgeQosReporterConfig) config;

				int outputGateIndex = edgeConfig.getOutputGateIndex();
				if (edgeConfig.isSourceTaskConfig() && outputGateIndex == outputIndex) {
					ensureOutputGateListener(writer, outputGateIndex);
				}
			}
		}
	}


	private void prepareVertexStatisticsReporters(VertexQosReporterConfig config) {
		QosReporterID.Vertex reporterID = QosReporterID.forVertex(this.task, config);

		LOG.debug("Installing vertex qos reporter {}: {}", reporterID, config);

		VertexStatisticsReportManager vertexStatisticsManager = getOrCreateVertexStatisticsManager();

		if (vertexStatisticsManager.containsReporter(reporterID)) {
			return;
		}

		int inputGateIndex = config.getInputGateIndex();
		int outputGateIndex = config.getOutputGateIndex();

		vertexStatisticsManager.addReporter(inputGateIndex, outputGateIndex, reporterID, config.getSamplingStrategy());
	}

	private void prepareEdgeStatisticsReporters(EdgeQosReporterConfig config) {
		IntermediateDataSetID dataSetID = config.getIntermediateDataSetID();
		boolean installed = false;

		LOG.debug("Installing edge qos reporter on {} with config {}", task.getName(), config);

		if (config.isSourceTaskConfig()) {
			int outputGateIndex = config.getOutputGateIndex();
			ResultPartitionWriter[] writers = this.taskEnvironment.getAllWriters();

			if (outputGateIndex < writers.length
					&& dataSetID.equals(writers[outputGateIndex].getDataSetId())) {

				ResultPartitionWriter writer = writers[outputGateIndex];
				IntermediateResultPartitionID partitionID = writer.getPartitionId().getPartitionId();

				for (int subIndex = 0; subIndex < writer.getNumberOfOutputChannels(); subIndex++) {
					QosReporterID.Edge reporterID = QosReporterID.forEdge(partitionID, subIndex);
					vertexStatisticsManager.addOutputGateReporter(outputGateIndex, subIndex, writer.getNumberOfOutputChannels(),
							reporterID);
					installed = true;
				}
			}
		} else {
			int inputGateIndex = config.getInputGateIndex();
			InputGate[] inputGates = this.taskEnvironment.getAllInputGates();
			if (inputGateIndex < inputGates.length
					&& (inputGates[inputGateIndex] instanceof SingleInputGate)
					&& dataSetID.equals(((SingleInputGate) inputGates[inputGateIndex]).getConsumedResultId())) {

				SingleInputGate inputGate = (SingleInputGate) inputGates[inputGateIndex];
				int subPartitionIndex = inputGate.getConsumedSubpartitionIndex();

				int i = 0;
				for (InputChannel channel : inputGate.getAllInputChannels()) {
					IntermediateResultPartitionID partitionID = channel.getPartitionId().getPartitionId();
					QosReporterID.Edge reporterID = QosReporterID.forEdge(partitionID, subPartitionIndex);
					vertexStatisticsManager.addInputGateReporter(inputGateIndex, i++, inputGate.getNumberOfInputChannels(),
							reporterID);
					installed = true;
				}
			}
		}

		if (!installed) {
			throw new RuntimeException("Failed to install EdgeQosReporter. This is bug.");
		}
	}

	private void ensureInputGateListener(QosReportingReader reader, int index) {
		InputGateQosReportingListener listener = reader.getQosCallback(index);
		if (listener == null) {
			listener = new InputGateListener(index);
			reader.setQosCallback(listener, index);
		}
	}

	private void ensureOutputGateListener(StreamRecordWriter outputGate, int outputGateIndex) {
		OutputGateQosReportingListener listener = outputGate.getQosCallback();
		if (listener == null) {
			listener = new OutputGateListener(outputGateIndex);
			outputGate.setQosCallback(listener);
		}
	}

	private class InputGateListener implements InputGateQosReportingListener {
		private int recordsReadFromBuffer = 0;
		private final int gateIndex;

		public InputGateListener(int gateIndex) {
			this.gateIndex = gateIndex;
		}

		@Override
		public void recordReceived(int channelIndex, TimeStampedRecord record) {
			vertexStatisticsManager.recordReceived(gateIndex);
			if (record.hasTimestamp()) {
				vertexStatisticsManager.reportLatenciesIfNecessary(gateIndex, channelIndex, record.getTimestamp());
			}
			++recordsReadFromBuffer;
		}

		@Override
		public void tryingToReadRecord() {
			vertexStatisticsManager.tryingToReadRecord(gateIndex);
		}

		@Override
		public void inputBufferConsumed(int channelIndex, long bufferInterarrivalTimeNanos) {
			vertexStatisticsManager.inputBufferConsumed(gateIndex, channelIndex, bufferInterarrivalTimeNanos,
					recordsReadFromBuffer);
		}
	}

	private class OutputGateListener implements OutputGateQosReportingListener {
		private final int gateIndex;

		public OutputGateListener(int gateIndex) {
			this.gateIndex = gateIndex;
		}

		@Override
		public void outputBufferSent(int channelIndex, long currentAmountTransmitted) {
			vertexStatisticsManager.outputBufferSent(gateIndex, channelIndex, currentAmountTransmitted);
		}

		@Override
		public void recordEmitted(int outputChannel, TimeStampedRecord record) {
			vertexStatisticsManager.recordEmitted(gateIndex, outputChannel, record);
		}

		@Override
		public void outputBufferAllocated(int channelIndex) {
			vertexStatisticsManager.outputBufferAllocated(gateIndex, channelIndex);
		}
	}
}
