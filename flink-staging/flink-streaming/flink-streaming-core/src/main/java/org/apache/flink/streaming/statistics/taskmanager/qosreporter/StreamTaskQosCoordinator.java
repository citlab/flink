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
import org.apache.flink.streaming.runtime.io.QosReportingRecordWriter;
import org.apache.flink.streaming.runtime.io.StreamingAbstractRecordReader;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.statistics.message.action.EdgeQosReporterConfig;
import org.apache.flink.streaming.statistics.message.action.QosReporterConfig;
import org.apache.flink.streaming.statistics.message.action.VertexQosReporterConfig;
import org.apache.flink.streaming.statistics.taskmanager.qosmodel.QosReporterID;
import org.apache.flink.streaming.statistics.taskmanager.qosreporter.listener.QosReportingListenerHelper;
import org.apache.flink.streaming.statistics.taskmanager.qosreporter.vertex.VertexStatisticsReportManager;
import org.apache.flink.streaming.statistics.util.QosStatisticsConfig;
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

	private InputGateReporterManager inputGateReporter;

	private OutputGateReporterManager outputGateReporter;

	/**
	 * For each input/output gate combination for which Qos reports are
	 * required, this {@link VertexStatisticsReportManager} creates the reports.
	 */
	private VertexStatisticsReportManager vertexStatisticsManager;

	public StreamTaskQosCoordinator(StreamTask task) {
		this.task = task;
		this.taskEnvironment = task.getEnvironment();
		this.forwarderThread = QosReportForwarderThread.getOrCreateForwarderAndRegisterTask(
				this, task, taskEnvironment);
	}

	public void prepareQosReporting() {
		for (QosReporterConfig config : this.task.getConfig().getQosReporterConfigs()) {
			if (config instanceof VertexQosReporterConfig) {
				installVertexStatisticsReporter((VertexQosReporterConfig) config);
			} else {
				installEdgeStatisticsReporter((EdgeQosReporterConfig) config);
			}
		}
	}

	public void cleanup() {
		this.forwarderThread.unregisterTask(this.task);
	}

	public long getAggregationInterval() {
		return this.task.getJobConfiguration().getLong(
				QosStatisticsConfig.AGGREGATION_INTERVAL_KEY,
				QosStatisticsConfig.getAggregationIntervalMillis());
	}

	public int getSamplingProbability() {
		return this.task.getJobConfiguration().getInteger(
				QosStatisticsConfig.SAMPLING_PROBABILITY_KEY,
				QosStatisticsConfig.getSamplingProbabilityPercent());
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

	public void setupInputQosListener(StreamingAbstractRecordReader<?> input) {
		List<QosReporterConfig> qosReporterConfigs = task.getConfig().getQosReporterConfigs();
		if (qosReporterConfigs.size() > 0) {
			for (QosReporterConfig config : qosReporterConfigs) {
				if (config instanceof VertexQosReporterConfig) {

					VertexQosReporterConfig vertexConfig = (VertexQosReporterConfig) config;

					IntermediateDataSetID inputDataSetID = vertexConfig.getInputDataSetID();
					final int inputGateIndex = vertexConfig.getInputGateIndex(); // must be -1 or 0
					if (inputDataSetID != null) {
						QosReportingListenerHelper.listenToVertexStatisticsOnInputGate(input, inputGateIndex,
								this.vertexStatisticsManager);
					}

				} else if (config instanceof EdgeQosReporterConfig) {
					EdgeQosReporterConfig edgeConfig = (EdgeQosReporterConfig) config;

					int inputGateIndex = edgeConfig.getInputGateIndex(); // must be -1 or 0
					if (inputGateIndex != -1) {
						QosReportingListenerHelper.listenToChannelLatenciesOnInputGate(input, inputGateReporter);
					}
				}
			}
		}
	}

	public void setupOutputQosListener(QosReportingRecordWriter<?> writer) {
		List<QosReporterConfig> qosReporterConfigs = task.getConfig().getQosReporterConfigs();
		if (qosReporterConfigs.size() > 0) {
			for (QosReporterConfig config : qosReporterConfigs) {
				if (config instanceof VertexQosReporterConfig) {

					VertexQosReporterConfig vertexConfig = (VertexQosReporterConfig) config;

					final VertexStatisticsReportManager vertexStatisticsManager = this.getOrCreateVertexStatisticsManager();

					IntermediateDataSetID outputDataSetID = vertexConfig.getOutputDataSetID();
					final int outputGateIndex = vertexConfig.getOutputGateIndex(); // must be -1 or 0;
					if (outputDataSetID != null) {
						QosReportingListenerHelper.listenToVertexStatisticsOnOutputGate(writer, outputGateIndex,
								vertexStatisticsManager);
					}

				} else if (config instanceof EdgeQosReporterConfig) {
					EdgeQosReporterConfig edgeConfig = (EdgeQosReporterConfig) config;

					int outputGateIndex = edgeConfig.getOutputGateIndex(); // must be -1 or 0
					if (outputGateIndex != -1) {
						QosReportingListenerHelper.listenToOutputChannelStatisticsOnOutputGate(writer, outputGateReporter);
					}
				}
			}
		}
	}


	private void installVertexStatisticsReporter(VertexQosReporterConfig config) {
		QosReporterID.Vertex reporterID = QosReporterID.forVertex(this.task, config);

		LOG.debug("Installing vertex qos reporter {}: {}", reporterID, config);

		getOrCreateVertexStatisticsManager();

		if (this.vertexStatisticsManager.containsReporter(reporterID)) {
			return;
		}

		int inputGateIndex = config.getInputGateIndex();
		int outputGateIndex = config.getOutputGateIndex();

		this.vertexStatisticsManager.addReporter(inputGateIndex, outputGateIndex, reporterID, config.getSamplingStrategy());
	}

	private void installEdgeStatisticsReporter(EdgeQosReporterConfig config) {
		IntermediateDataSetID dataSetID = config.getIntermediateDataSetID();
		boolean installed = false;

		// find input gate
		int inputGateIndex = config.getInputGateIndex();
		InputGate[] inputGates = this.taskEnvironment.getAllInputGates();
		if (inputGateIndex < inputGates.length
				&& (inputGates[inputGateIndex] instanceof SingleInputGate)
				&& dataSetID.equals(((SingleInputGate)inputGates[inputGateIndex]).getConsumedResultId())) {

			SingleInputGate inputGate = (SingleInputGate) inputGates[inputGateIndex];
			int subPartitionIndex = this.task.getIndexInSubtaskGroup();

			inputGateReporter = new InputGateReporterManager(forwarderThread,
					inputGate.getNumberOfInputChannels());

			int i = 0;
			for (InputChannel channel : inputGate.getAllInputChannels()) {
				IntermediateResultPartitionID partitionID = channel.getPartitionId().getPartitionId();
				QosReporterID.Edge reporterID = QosReporterID.forEdge(partitionID, subPartitionIndex);
				inputGateReporter.addEdgeQosReporterConfig(i++, reporterID);
				installed = true;
			}
		}


		// find output gate
		if (!installed) {
			int outputGateIndex = config.getOutputGateIndex();
			ResultPartitionWriter[] writers = this.taskEnvironment.getAllWriters();

			if (outputGateIndex < writers.length
					&& dataSetID.equals(writers[outputGateIndex].getDataSetId())) {

				ResultPartitionWriter writer = writers[outputGateIndex];
				IntermediateResultPartitionID partitionID = writer.getPartitionId().getPartitionId();

				outputGateReporter = new OutputGateReporterManager(forwarderThread,
						writer.getNumberOfOutputChannels());

				for (int subIndex = 0; subIndex < writer.getNumberOfOutputChannels(); subIndex++) {
					QosReporterID.Edge reporterID = QosReporterID.forEdge(partitionID, subIndex);
					outputGateReporter.addEdgeQosReporterConfig(subIndex, reporterID);
					installed = true;
				}
			}
		}

		if (!installed) {
			throw new RuntimeException("Failed to install EdgeQosReporter. This is bug.");
		}
	}
}
