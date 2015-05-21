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
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.statistics.message.action.EdgeQosReporterConfig;
import org.apache.flink.streaming.statistics.message.action.QosReporterConfig;
import org.apache.flink.streaming.statistics.message.action.VertexQosReporterConfig;
import org.apache.flink.streaming.statistics.taskmanager.qosmodel.QosReporterID;
import org.apache.flink.streaming.statistics.taskmanager.qosreporter.vertex.VertexStatisticsReportManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

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
	private ArrayList<InputGateReporterManager> inputGateReporters;

	/**
	 * For each output gate of the task for whose output channels QoS statistics
	 * are required (throughput, output buffer lifetime, ...), this list
	 * contains a OutputGateReporterManager. Each OutputGateReporterManager
	 * keeps track of and reports on Qos statistics all of the output gate's
	 * channels and also attaches tags to records sent via its channels. This is
	 * a sparse list (may contain nulls), indexed by the runtime gate's own
	 * indices.
	 */
	private ArrayList<OutputGateReporterManager> outputGateReporters;

	/**
	 * For each input/output gate combination for which Qos reports are
	 * required, this {@link VertexStatisticsReportManager} creates the reports.
	 */
	private VertexStatisticsReportManager vertexStatisticsManager;

	public StreamTaskQosCoordinator(StreamTask task) {
		this.task = task;
		this.taskEnvironment = task.getEnvironment();
		this.forwarderThread =
			QosReportForwarderThread.getOrCreateForwarderAndRegisterTask(task, taskEnvironment);
		this.inputGateReporters = new ArrayList<InputGateReporterManager>();
		this.outputGateReporters = new ArrayList<OutputGateReporterManager>();
	}

	public void prepareQosReporting() {
		for (QosReporterConfig config : this.task.getConfig().getQosReporterConfigs()) {
			if (config instanceof VertexQosReporterConfig) {
				installVertexStatisticsReporters((VertexQosReporterConfig) config);
			} else {
				installEdgeStatisticsReporters((EdgeQosReporterConfig) config);
			}
		}
	}

	public void cleanup() {
		this.forwarderThread.unregisterTask(this.task);
	}


//	public synchronized void handleLimitBufferSizeAction(
//			LimitBufferSizeAction limitBufferSizeAction) {
//		TODO
//	}

//	public void handleSetOutputLatencyTargetAction(
//			SetOutputBufferLifetimeTargetAction action) {
//		TODO
//	}

	private void installVertexStatisticsReporters(VertexQosReporterConfig config) {
		QosReporterID.Vertex reporterID = QosReporterID.forVertex(this.task, config);
		installVertexStatisticsReporter(reporterID, config);
	}

	private void installEdgeStatisticsReporters(EdgeQosReporterConfig config) {
		IntermediateDataSetID dataSetID = config.getIntermediateDataSetID();
		boolean installed = false;

		if (config.isSourceTaskConfig()) {
			int outputGateIndex = config.getOutputGateIndex();
			ResultPartitionWriter[] writers = this.taskEnvironment.getAllWriters();

			if (outputGateIndex < writers.length
					&& dataSetID.equals(writers[outputGateIndex].getDataSetId())) {

				ResultPartitionWriter writer = writers[outputGateIndex];
				IntermediateResultPartitionID partitionID = writer.getPartitionId().getPartitionId();

				for (int subIndex = 0; subIndex < writer.getNumberOfOutputChannels(); subIndex++) {
					QosReporterID.Edge reporterID = QosReporterID.forEdge(partitionID, subIndex);
					installOutputGateListeners(reporterID, config);
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
				int subPartitionIndex = this.task.getIndexInSubtaskGroup();

				for (InputChannel channel : inputGate.getAllInputChannels()) {
					IntermediateResultPartitionID partitionID = channel.getPartitionId().getPartitionId();
					QosReporterID.Edge reporterID = QosReporterID.forEdge(partitionID, subPartitionIndex);
					installInputGateListeners(reporterID, config);
					installed = true;
				}
			}
		}

		if (!installed) {
			throw new RuntimeException("Failed to install EdgeQosReporter. This is bug.");
		}
	}

	private void installVertexStatisticsReporter(
			QosReporterID.Vertex reporterID, VertexQosReporterConfig reporterConfig) {

		LOG.debug("Installing vertex qos reporter {}: {}", reporterID, reporterConfig);

		if (this.vertexStatisticsManager == null) {
			this.vertexStatisticsManager = new VertexStatisticsReportManager(
				this.forwarderThread,
				this.taskEnvironment.getAllInputGates().length,
				this.taskEnvironment.getAllWriters().length);
		}

		// TODO: register listeners
	}

	private void installInputGateListeners(
			QosReporterID.Edge reporterID, EdgeQosReporterConfig reporterConfig) {

		LOG.debug("Installing edge qos reporter {}: {}", reporterID, reporterConfig);

		// TODO: register listeners
	}

	private void installOutputGateListeners(
			QosReporterID.Edge reporterID, EdgeQosReporterConfig reporterConfig) {

		LOG.debug("Installing edge qos reporter {}: {}", reporterID, reporterConfig);

		// TODO: register listeners
	}
}
