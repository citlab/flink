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

/**
 * Instances of this class keep track of the Qos reporter configuration of a job
 * on a task manager. This consists of things like the aggregation interval but
 * also which vertices/edges to monitor and report on. Since the Qos reporter
 * configuration for a particular vertex (channel) can arrive before or after
 * the vertex is started or the edge ships the first records, listeners can be
 * set that are when new Qos reporter configuration arrives.
 *
 * All methods of this class are thread-safe, but if you need to do things like
 * "check for configuration and if none exists, install a listener" which
 * consists of multiple method calls, then you should do this in a synchronized
 * block, that locks the instance of this class you are using.
 *
 * @author Bjoern Lohrmann
 */
public class QosReporterConfigCenter {

//	private HashMap<AbstractID, QosReporterConfigListener> configListeners;
//
//	private HashMap<ExecutionAttemptID, Set<VertexQosReporterConfig>> reporterConfigsByExecutionVertex;
//
//	private HashMap<ResultPartitionID, EdgeQosReporterConfig> reporterConfigsByResultPartition;
//
//	private volatile long aggregationInterval;
//
//	private volatile int samplingProbability;
//
//	public QosReporterConfigCenter(Configuration jobConfiguration) {
//		this.reporterConfigsByExecutionVertex = new HashMap<ExecutionAttemptID, Set<VertexQosReporterConfig>>();
//		this.reporterConfigsByResultPartition = new HashMap<ResultPartitionID, EdgeQosReporterConfig>();
//		this.configListeners = new HashMap<AbstractID, QosReporterConfigListener>();
//
//		// TODO: read aggregationInterval and samplingProbability from jobConfiguration
//	}
//
//
//
//	public synchronized void setQosReporterConfigListener(
//			ExecutionAttemptID executionAttemptID, QosReporterConfigListener listener) {
//
//		this.configListeners.put(executionAttemptID, listener);
//	}
//
//	public synchronized void unsetQosReporterConfigListener(
//			ExecutionAttemptID executionAttemptID) {
//		this.configListeners.remove(executionAttemptID);
//	}
//
//	public synchronized void addVertexQosReporter(
//			VertexQosReporterConfig newReporter) {
//		Set<VertexQosReporterConfig> reporters = this.reporterConfigsByExecutionVertex
//				.get(newReporter.getExecutionAttemptID());
//
//		if (reporters == null) {
//			reporters = new HashSet<VertexQosReporterConfig>();
//			this.reporterConfigsByExecutionVertex.put(
//					newReporter.getExecutionAttemptID(), reporters);
//		}
//		reporters.add(newReporter);
//
//		QosReporterConfigListener listener = this.configListeners
//				.get(newReporter.getExecutionAttemptID());
//		if (listener != null) {
//			listener.newVertexQosReporter(newReporter);
//		}
//	}
//
//	public synchronized void addEdgeQosReporter(
//			EdgeQosReporterConfig newReporter) {
//
//		EdgeQosReporterConfig oldReporter = this.reporterConfigsByResultPartition
//				.get(newReporter.getIntermediateResultPartitionID());
//
//		if (oldReporter != null) {
//			// TODO
//			throw new RuntimeException("huch?!");
//			EdgeQosReporterConfig merged =
//					this.mergeEdgeQosReporterConfigs(oldReporter, newReporter);
//			this.reporterConfigsByResultPartition.put(
//					merged.getIntermediateResultPartitionID(), merged);
//
//			// send no notification about merge because only report forwarder is
//			// interested in what has been merged
//		} else {
//			this.reporterConfigsByResultPartition.put(newReporter.getIntermediateResultPartitionID(),
//					newReporter);
//
//			if (this.configListeners.containsKey(newReporter.getIntermediateResultPartitionID()))
//			if (this.configListeners.containsKey(newReporter.getInputGateID())) {
//				this.configListeners.get(newReporter.getInputGateID())
//						.newEdgeQosReporter(newReporter);
//			}
//
//			if (this.configListeners.containsKey(newReporter.getOutputGateID())) {
//				this.configListeners.get(newReporter.getOutputGateID())
//						.newEdgeQosReporter(newReporter);
//			}
//		}
//	}
//
//
//	public synchronized Set<VertexQosReporterConfig> getVertexQosReporters(
//			ExecutionVertexID vertexID) {
//
//		Set<VertexQosReporterConfig> reporters = this.reporterConfigsByExecutionVertex
//				.get(vertexID);
//
//		if (reporters != null) {
//			return new HashSet<VertexQosReporterConfig>(reporters);
//		}
//		return Collections.emptySet();
//	}
//
//	public synchronized EdgeQosReporterConfig getEdgeQosReporter(
//			ChannelID channelID) {
//		return this.reporterConfigsByChannel.get(channelID);
//	}
//
//	/**
//	 * Returns the aggregationInterval.
//	 *
//	 * @return the aggregationInterval
//	 */
//	public long getAggregationInterval() {
//		return this.aggregationInterval;
//	}
//
//	/**
//	 * Sets the aggregationInterval to the specified value.
//	 *
//	 * @param aggregationInterval
//	 *            the aggregationInterval to set
//	 */
//	public void setAggregationInterval(long aggregationInterval) {
//		this.aggregationInterval = aggregationInterval;
//	}
//
//	/**
//	 * Returns the samplingProbability.
//	 *
//	 * @return the samplingProbability
//	 */
//	public int getSamplingProbability() {
//		return this.samplingProbability;
//	}
//
//	/**
//	 * Sets the samplingProbability to the specified value.
//	 *
//	 * @param samplingProbability
//	 *            the samplingProbability to set
//	 */
//	public void setSamplingProbability(int samplingProbability) {
//		this.samplingProbability = samplingProbability;
//	}
//
//	public VertexQosReporterConfig getVertexQosReporter(
//			QosReporterID.Vertex reporterID) {
//		Set<VertexQosReporterConfig> configs = this
//				.getVertexQosReporters(reporterID.getVertexID());
//
//		for (VertexQosReporterConfig config : configs) {
//			if (config.getReporterID().equals(reporterID)) {
//				return config;
//			}
//		}
//		return null;
//	}
//
//	public EdgeQosReporterConfig getEdgeQosReporter(
//			QosReporterID.Edge reporterID) {
//		return this.getEdgeQosReporter(reporterID.getSourceChannelID());
//	}
}
