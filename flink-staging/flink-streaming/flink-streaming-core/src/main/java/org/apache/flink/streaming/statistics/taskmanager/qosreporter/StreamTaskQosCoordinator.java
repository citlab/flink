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
import org.apache.flink.streaming.statistics.taskmanager.qosreporter.listener.QosReportingListenerHelper;
import org.apache.flink.streaming.statistics.taskmanager.qosreporter.vertex.VertexStatisticsReportManager;
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
	 * For each input gate of the task for whose channels latency reporting is
	 * required, this list contains a InputGateReporterManager. A
	 * InputGateReporterManager keeps track of and reports on the latencies for
	 * all of the input gate's channels. This is a sparse list (may contain
	 * nulls), indexed by the runtime gate's own indices.
	 */
	private InputGateReporterManager[] inputGateReporters;

	/**
	 * For each output gate of the task for whose output channels QoS statistics
	 * are required (throughput, output buffer lifetime, ...), this list
	 * contains a OutputGateReporterManager. Each OutputGateReporterManager
	 * keeps track of and reports on Qos statistics all of the output gate's
	 * channels and also attaches tags to records sent via its channels. This is
	 * a sparse list (may contain nulls), indexed by the runtime gate's own
	 * indices.
	 */
	private OutputGateReporterManager[] outputGateReporters;

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
		this.inputGateReporters = new InputGateReporterManager[this.taskEnvironment.getAllInputGates().length];
		this.outputGateReporters = new OutputGateReporterManager[this.taskEnvironment.getAllWriters().length];
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
					QosReportingListenerHelper.listenToVertexStatisticsOnInputGate(input, inputGateIndex,
							vertexStatisticsManager);
				}

			} else if (config instanceof EdgeQosReporterConfig) {
				EdgeQosReporterConfig edgeConfig = (EdgeQosReporterConfig) config;
				int inputGateIndex = edgeConfig.getInputGateIndex();

				if (edgeConfig.isTargetTaskConfig()) {
					InputGateReporterManager inputGateReporter = inputGateReporters[inputGateIndex];
					QosReportingListenerHelper.listenToChannelLatenciesOnInputGate(input, inputGateIndex, inputGateReporter);
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
					QosReportingListenerHelper.listenToVertexStatisticsOnOutputGate(writer, outputGateIndex,
							vertexStatisticsManager);
				}

			} else if (config instanceof EdgeQosReporterConfig) {
				EdgeQosReporterConfig edgeConfig = (EdgeQosReporterConfig) config;

				int outputGateIndex = edgeConfig.getOutputGateIndex();
				if (edgeConfig.isSourceTaskConfig() && outputGateIndex == outputIndex) {
					OutputGateReporterManager outputGateReporter = outputGateReporters[outputGateIndex];
					QosReportingListenerHelper.listenToOutputChannelStatisticsOnOutputGate(writer, outputGateReporter);
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

				OutputGateReporterManager outputGateReporter = new OutputGateReporterManager(forwarderThread,
						writer.getNumberOfOutputChannels());

				for (int subIndex = 0; subIndex < writer.getNumberOfOutputChannels(); subIndex++) {
					QosReporterID.Edge reporterID = QosReporterID.forEdge(partitionID, subIndex);
					outputGateReporter.addEdgeQosReporterConfig(subIndex, reporterID);
					installed = true;
				}

				outputGateReporters[outputGateIndex] = outputGateReporter;
			}
		} else {
			int inputGateIndex = config.getInputGateIndex();
			InputGate[] inputGates = this.taskEnvironment.getAllInputGates();
			if (inputGateIndex < inputGates.length
					&& (inputGates[inputGateIndex] instanceof SingleInputGate)
					&& dataSetID.equals(((SingleInputGate) inputGates[inputGateIndex]).getConsumedResultId())) {

				SingleInputGate inputGate = (SingleInputGate) inputGates[inputGateIndex];
				int subPartitionIndex = inputGate.getConsumedSubpartitionIndex();

				InputGateReporterManager inputGateReporter = new InputGateReporterManager(forwarderThread,
						inputGate.getNumberOfInputChannels());

				int i = 0;
				for (InputChannel channel : inputGate.getAllInputChannels()) {
					IntermediateResultPartitionID partitionID = channel.getPartitionId().getPartitionId();
					QosReporterID.Edge reporterID = QosReporterID.forEdge(partitionID, subPartitionIndex);
					inputGateReporter.addEdgeQosReporterConfig(i++, reporterID);
					installed = true;
				}

				inputGateReporters[inputGateIndex] = inputGateReporter;
			}
		}

		if (!installed) {
			throw new RuntimeException("Failed to install EdgeQosReporter. This is bug.");
		}
	}
}
