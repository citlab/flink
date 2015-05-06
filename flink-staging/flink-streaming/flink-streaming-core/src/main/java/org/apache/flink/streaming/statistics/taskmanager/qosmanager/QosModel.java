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

package org.apache.flink.streaming.statistics.taskmanager.qosmanager;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionEdge;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.statistics.JobGraphLatencyConstraint;
import org.apache.flink.streaming.statistics.JobGraphSequence;
import org.apache.flink.streaming.statistics.LatencyConstraintID;
import org.apache.flink.streaming.statistics.SequenceElement;
import org.apache.flink.streaming.statistics.message.action.EdgeQosReporterConfig;
import org.apache.flink.streaming.statistics.message.action.QosReporterConfig;
import org.apache.flink.streaming.statistics.message.action.VertexQosReporterConfig;
import org.apache.flink.streaming.statistics.message.qosreport.EdgeLatency;
import org.apache.flink.streaming.statistics.message.qosreport.EdgeStatistics;
import org.apache.flink.streaming.statistics.message.qosreport.QosReport;
import org.apache.flink.streaming.statistics.message.qosreport.VertexStatistics;
import org.apache.flink.streaming.statistics.taskmanager.qosmodel.EdgeQosData;
import org.apache.flink.streaming.statistics.taskmanager.qosmodel.QosEdge;
import org.apache.flink.streaming.statistics.taskmanager.qosmodel.QosGate;
import org.apache.flink.streaming.statistics.taskmanager.qosmodel.QosGraph;
import org.apache.flink.streaming.statistics.taskmanager.qosmodel.QosGraphMember;
import org.apache.flink.streaming.statistics.taskmanager.qosmodel.QosGroupVertex;
import org.apache.flink.streaming.statistics.taskmanager.qosmodel.QosReporterID;
import org.apache.flink.streaming.statistics.taskmanager.qosmodel.QosVertex;
import org.apache.flink.streaming.statistics.taskmanager.qosmodel.VertexQosData;
import org.apache.flink.streaming.statistics.util.QosStatisticsConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Wrapper class around a Qos graph used by a Qos manager. A Qos model is a
 * state machine that first assembles a Qos graph from
 * {@link EdgeQosReporterConfig} and {@link VertexQosReporterConfig} objects and
 * then continuously adds Qos report data to the Qos graph. It can then be used
 * to search for violated Qos constraints inside the Qos graph.
 * 
 * @author Bjoern Lohrmann, Sascha Wolke
 */
public class QosModel {

	public enum State {
		/**
		 * If the Qos model is shallow, it means that the internal Qos graph
		 * does contain group vertices, but at least one group vertex has no
		 * members. Members are added by vertex/edge announcements piggybacked
		 * inside of Qos reports from the Qos reporters.
		 */
		SHALLOW,

		/**
		 * If the Qos model is ready, it means that the internal Qos graph does
		 * contain group vertices, and each group vertex has at least one member
		 * vertex. A transition back to SHALLOW is possible, when new shallow
		 * group vertices are merged into the Qos graph. Members may still be
		 * added by vertex/edge announcements piggybacked inside of Qos reports
		 * at any time.
		 */
		READY
	}

	private State state;

	/**
	 * A sparse graph that is assembled from two sources: (1) The (shallow)
	 * group-level Qos graphs received as part of the Qos manager roles
	 * delivered by job manager. (2) The vertex/edge reporter announcements
	 * delivered by (possibly many) Qos reporters, once the vertex/edge produces
	 * Qos data (which is may never happen, especially for some edges).
	 */
	private QosGraph qosGraph;

	/**
	 * Reassemble graph based on state changes.
	 */
	private final LinkedBlockingQueue<Execution> pendingVertexStateChanges;

	/**
	 * A dummy object containing chain updates that need to be buffered, because
	 * the edges affected by the update are not yet in the QoS graph.
	 */
//	private final ChainUpdates chainUpdatesBuffer;

	/**
	 * All Qos vertices and edges by reporter ID.
	 */
	private HashMap<QosReporterID, QosGraphMember> graphMemberByReporterID;

	private HashMap<QosReporterID, QosReporterConfig> reporterConfigByID;

	public QosModel(QosGraph qosGraph) {
		this.qosGraph = qosGraph;
		this.state = State.SHALLOW;
		this.pendingVertexStateChanges = new LinkedBlockingQueue<Execution>();
//		this.chainUpdatesBuffer = new ChainUpdates(jobID);
		this.graphMemberByReporterID = new HashMap<QosReporterID, QosGraphMember>();
		this.reporterConfigByID = new HashMap<QosReporterID, QosReporterConfig>();
	}

	public boolean isReady() {
		return this.state == State.READY;
	}

	public boolean isShallow() {
		return this.state == State.SHALLOW;
	}

	public void processQosReport(QosReport report) {
		switch (this.state) {
		case READY:
			this.processAnnouncements();
			this.processQosRecords(report);
			break;
		case SHALLOW:
			this.processAnnouncements();
			break;
		}
	}

	private void processAnnouncements() {
		if (!this.pendingVertexStateChanges.isEmpty()) {
			this.processVertexStatusChanges();
		}
//		this.tryToProcessBufferedChainUpdates();
	}

//	public void processChainUpdates(ChainUpdates announce) {
//		this.bufferChainUpdates(announce);
//		this.tryToProcessBufferedChainUpdates();
//	}
//
//	private void tryToProcessBufferedChainUpdates() {
//		Iterator<QosReporterID.Edge> unchainedIter = this.chainUpdatesBuffer.getUnchainedEdges().iterator();
//		while (unchainedIter.hasNext()) {
//
//			QosReporterID.Edge edgeReporterID = unchainedIter.next();
//			QosEdge edge = this.edgeBySourceChannelID.get(edgeReporterID
//					.getSourceChannelID());
//
//			if (edge == null) {
//				continue;
//			}
//
//			edge.getQosData().setIsInChain(false);
//
//			unchainedIter.remove();
//			Logger.getLogger(this.getClass()).info(
//					"Edge " + edge + " has been unchained.");
//		}
//
//		Iterator<QosReporterID.Edge> newlyChainedIter = this.chainUpdatesBuffer
//				.getNewlyChainedEdges().iterator();
//		while (newlyChainedIter.hasNext()) {
//
//			QosReporterID.Edge edgeReporterID = newlyChainedIter.next();
//
//			QosEdge edge = this.edgeBySourceChannelID.get(edgeReporterID
//					.getSourceChannelID());
//
//			if (edge == null) {
//				continue;
//			}
//
//			edge.getQosData().setIsInChain(true);
//
//			newlyChainedIter.remove();
//			Logger.getLogger(this.getClass()).info(
//					"Edge " + edge + " has been chained.");
//		}
//	}
//
//	private void bufferChainUpdates(ChainUpdates announce) {
//		this.chainUpdatesBuffer.getUnchainedEdges().addAll(
//				announce.getUnchainedEdges());
//		this.chainUpdatesBuffer.getNewlyChainedEdges().addAll(
//				announce.getNewlyChainedEdges());
//	}

	private void processQosRecords(QosReport report) {
		long now = System.currentTimeMillis();
		this.processVertexStatistics(report.getVertexStatistics(), now);
		this.processEdgeStatistics(report.getEdgeStatistics(), now);
		this.processEdgeLatencies(report.getEdgeLatencies(), now);
	}

	private void processVertexStatistics(Collection<VertexStatistics> vertexLatencies, long now) {
		for (VertexStatistics vertexStats : vertexLatencies) {
			QosGraphMember vertex = this.graphMemberByReporterID.get(vertexStats.getReporterID());
			QosReporterConfig reporterConfig = this.reporterConfigByID.get(vertexStats.getReporterID());

			if (vertex != null) {
				vertex.processStatistics(reporterConfig, vertexStats, now);
			}
		}
	}

	private void processEdgeStatistics(Collection<EdgeStatistics> edgeStatistics, long now) {
		for (EdgeStatistics edgeStatistic : edgeStatistics) {
			QosGraphMember edge = this.graphMemberByReporterID.get(edgeStatistic.getReporterID());
			QosReporterConfig reporterConfig = this.reporterConfigByID.get(edgeStatistic.getReporterID());

			if (edge != null) {
				edge.processStatistics(reporterConfig, edgeStatistic, now);
			}
		}
	}

	private void processEdgeLatencies(Collection<EdgeLatency> edgeLatencies, long now) {
		for (EdgeLatency edgeLatency : edgeLatencies) {
			QosGraphMember edge = this.graphMemberByReporterID.get(edgeLatency.getReporterID());
			QosReporterConfig reporterConfig = this.reporterConfigByID.get(edgeLatency.getReporterID());

			if (edge != null) {
				edge.processStatistics(reporterConfig, edgeLatency, now);
			}
		}
	}

	public void handOffVertexStatusChange(Execution vertex) {
		synchronized (this.pendingVertexStateChanges) {
			this.pendingVertexStateChanges.add(vertex);
		}
	}

	private void processVertexStatusChanges() {
		ArrayList<Execution> pendingChanges = new ArrayList<Execution>();

		synchronized (this.pendingVertexStateChanges) {
			pendingChanges.ensureCapacity(this.pendingVertexStateChanges.size());
			this.pendingVertexStateChanges.drainTo(pendingChanges);
		}

		if (!pendingChanges.isEmpty()) {
			for (Execution execution : pendingChanges) {
				ExecutionVertex vertex = execution.getVertex();
				ExecutionState state = vertex.getExecutionState();

				if (state == ExecutionState.RUNNING) {
					assembleFromExecutionVertex(vertex);
				} else if (state.isTerminal()) {
					// TODO: remove from graph
				}
			}

			if (this.qosGraph.isShallow()) {
				this.state = State.SHALLOW;
			} else {
				this.state = State.READY;
			}
		}
	}

	/**
	 * Assemble qos vertex, gates and edges from execution vertex.
	 */
	private void assembleFromExecutionVertex(ExecutionVertex vertex) {
		StreamConfig streamConfig = new StreamConfig(vertex.getJobVertex().getJobVertex().getConfiguration());

		if (!streamConfig.hasQosReporterConfigs()) {
			return;
		}

		QosGroupVertex groupVertex = this.qosGraph.getGroupVertexByID(vertex.getJobvertexId());
		int memberIndex = vertex.getParallelSubtaskIndex();
		QosVertex memberVertex = new QosVertex(vertex);
		memberVertex.setQosData(new VertexQosData(memberVertex));
		groupVertex.setGroupMember(memberVertex);

		// First step: Assemble gates from vertex configs
		for(QosReporterConfig config : streamConfig.getQosReporterConfigs()) {
			if (config instanceof VertexQosReporterConfig) {
				VertexQosReporterConfig vertexConfig = (VertexQosReporterConfig) config;
				QosReporterID reporterID = QosReporterID.forVertex(memberIndex, vertexConfig);
				this.graphMemberByReporterID.put(reporterID, memberVertex);
				this.reporterConfigByID.put(reporterID, vertexConfig);
				assembleQosGatesFromConfig(memberVertex, vertexConfig);
			}
		}

		// Second step: Assemble edges from edge configs
		for(QosReporterConfig config : streamConfig.getQosReporterConfigs()) {
			if (config instanceof EdgeQosReporterConfig) {
				EdgeQosReporterConfig edgeConfig = (EdgeQosReporterConfig) config;
				assembleQosEdgesFromConfig(vertex, edgeConfig);
			}
		}

	}

	private void assembleQosGatesFromConfig(QosVertex memberVertex, VertexQosReporterConfig toProcess) {
		int inputGateIndex = toProcess.getInputGateIndex();
		int outputGateIndex = toProcess.getOutputGateIndex();

		// if the reporter config has a previously unknown input gate
		// for us, add it to the vertex
		if (inputGateIndex != -1 && memberVertex.getInputGate(inputGateIndex) == null) {
			QosGate gate = toProcess.toInputGate();
			memberVertex.setInputGate(gate);
		}

		// if the reporter config has a previously unknown output gate
		// for us, add it to the vertex
		if (outputGateIndex != -1 && memberVertex.getOutputGate(outputGateIndex) == null) {
			QosGate gate = toProcess.toOutputGate();
			memberVertex.setOutputGate(gate);
		}

		// only if the reporter has a valid input/output gate combination,
		// prepare for reports on that combination
		if (inputGateIndex != -1 && outputGateIndex != -1) {
			memberVertex.getQosData().prepareForReportsOnGateCombination(
					inputGateIndex, outputGateIndex);

		} else if (inputGateIndex != -1) {
			memberVertex.getQosData().prepareForReportsOnInputGate(inputGateIndex);
		} else {
			memberVertex.getQosData().prepareForReportsOnOutputGate(outputGateIndex);
		}
	}

	private void assembleQosEdgesFromConfig(ExecutionVertex memberVertex, EdgeQosReporterConfig toProcess) {
		if (toProcess.isTargetTaskConfig()) {
			ExecutionEdge[] executionEdges = memberVertex.getInputEdges(toProcess.getInputGateIndex());

			for (ExecutionEdge executionEdge : executionEdges) {
				QosReporterID.Edge reporterID = QosReporterID.forEdge(
					executionEdge.getSource().getPartitionId(),
					memberVertex.getParallelSubtaskIndex()
				);

				if (!this.graphMemberByReporterID.containsKey(reporterID)) {
					ExecutionVertex sourceVertex = executionEdge.getSource().getProducer();
					QosEdge qosEdge = assembleQosEdgeFromConfig(
						sourceVertex, memberVertex, reporterID, toProcess
					);
					this.graphMemberByReporterID.put(reporterID, qosEdge);
					this.reporterConfigByID.put(reporterID, toProcess);
				}
			}
		}
	}

	// TODO: merge this with assembleQosEdgesFromConfig
	private QosEdge assembleQosEdgeFromConfig(
			ExecutionVertex sourceVertex, ExecutionVertex targetVertex,
			QosReporterID.Edge reporterID, EdgeQosReporterConfig toProcess) {

		int sourceTaskIndex = sourceVertex.getParallelSubtaskIndex();
		int targetTaskIndex = targetVertex.getParallelSubtaskIndex();

		QosGroupVertex sourceGroupVertex = this.qosGraph.getGroupVertexByID(sourceVertex.getJobvertexId());
		QosGroupVertex targetGroupVertex = this.qosGraph.getGroupVertexByID(targetVertex.getJobvertexId());

		QosVertex sourceQosVertex = sourceGroupVertex.getMember(sourceTaskIndex);
		QosVertex targetQosVertex = targetGroupVertex.getMember(targetTaskIndex);

		QosGate outputGate = sourceQosVertex.getOutputGate(toProcess.getOutputGateIndex());
		QosGate inputGate = targetQosVertex.getInputGate(toProcess.getInputGateIndex());

		QosEdge qosEdge = new QosEdge(reporterID);
		qosEdge.setOutputGate(outputGate);
		qosEdge.setInputGate(inputGate);
		qosEdge.setQosData(new EdgeQosData(qosEdge));

		return qosEdge;
	}

	public List<QosConstraintSummary> findQosConstraintViolationsAndSummarize(
			QosConstraintViolationListener listener) {
		
		long now = System.currentTimeMillis();

		long inactivityThresholdTime = now - 2 * QosStatisticsConfig.getAdjustmentIntervalMillis();

		List<QosConstraintSummary> constraintSummaries = new LinkedList<QosConstraintSummary>();

		for (JobGraphLatencyConstraint constraint : this.qosGraph.getConstraints()) {

			QosConstraintViolationFinder constraintViolationFinder = new QosConstraintViolationFinder(
					constraint.getID(), this.qosGraph, listener,
					inactivityThresholdTime);

			QosConstraintViolationReport violationReport = constraintViolationFinder
					.scanSequencesForQosConstraintViolations();

			constraintSummaries.add(createConstraintSummary(constraint,
					violationReport, inactivityThresholdTime));
		}

		return constraintSummaries;
	}

	private QosConstraintSummary createConstraintSummary(
			JobGraphLatencyConstraint constraint,
			QosConstraintViolationReport violationReport,
			long inactivityThresholdTime) {

		QosConstraintSummary constraintSummary = new QosConstraintSummary(
				constraint, violationReport);

		JobGraphSequence seq = constraint.getSequence();

		for (SequenceElement seqElem : seq) {
			if (seqElem.isVertex()) {
				summarizeGroupVertex(seqElem,
						constraintSummary.getGroupVertexSummary(
								seqElem.getIndexInSequence()), inactivityThresholdTime);

			} else {
				summarizeGroupEdge(seqElem,
						constraintSummary.getGroupEdgeSummary(
								seqElem.getIndexInSequence()), inactivityThresholdTime);
			}
		}

		fixupGroupEdgeSummaries(constraint, constraintSummary,
				inactivityThresholdTime);

		return constraintSummary;
	}

	private void fixupGroupEdgeSummaries(JobGraphLatencyConstraint constraint,
			QosConstraintSummary constraintSummary, long inactivityThresholdTime) {

		JobGraphSequence seq = constraint.getSequence();

		for (SequenceElement seqElem : seq) {
			if (seqElem.isEdge()) {
				int succIndex = seqElem.getIndexInSequence() + 1;
				QosGroupEdgeSummary toFix = constraintSummary
						.getGroupEdgeSummary(seqElem.getIndexInSequence());
				
				QosGroupVertexSummary succSummary;

				if (succIndex < seq.size()) {
					succSummary = constraintSummary.getGroupVertexSummary(succIndex);

				} else {
					succSummary = new QosGroupVertexSummary();
					summarizeGroupVertex(succSummary, inactivityThresholdTime,
							seqElem.getInputGateIndex(), -1,
							qosGraph.getGroupVertexByID(seqElem.getTargetVertexID()));
				}
				
				toFix.setMeanConsumerVertexLatency(succSummary.getMeanVertexLatency());
				toFix.setMeanConsumerVertexLatencyCV(succSummary.getMeanVertexLatencyCV());
			} 
		}
	}

	private void summarizeGroupVertex(SequenceElement seqElem,
			QosGroupVertexSummary groupVertexSummary,
			long inactivityThresholdTime) {

		int inputGateIndex = seqElem.getInputGateIndex();
		int outputGateIndex = seqElem.getInputGateIndex();
		QosGroupVertex groupVertex = qosGraph.getGroupVertexByID(seqElem.getVertexID());

		summarizeGroupVertex(groupVertexSummary, inactivityThresholdTime,
				inputGateIndex, outputGateIndex, groupVertex);
	}

	private void summarizeGroupVertex(QosGroupVertexSummary groupVertexSummary,
			long inactivityThresholdTime, int inputGateIndex,
			int outputGateIndex, QosGroupVertex groupVertex) {

		int activeVertices = 0;
		double vertexLatencySum = 0;
		double vertexLatencyCASum = 0;

		for (QosVertex memberVertex : groupVertex.getMembers()) {
			VertexQosData qosData = memberVertex.getQosData();

			if (qosData.hasNewerData(inputGateIndex, outputGateIndex, inactivityThresholdTime)) {
				activeVertices++;
				vertexLatencySum += qosData.getLatencyInMillis(inputGateIndex);
				vertexLatencyCASum += qosData.getLatencyCV(inputGateIndex);
			}
		}

		if (activeVertices > 0) {
			groupVertexSummary.setActiveVertices(activeVertices);
			groupVertexSummary.setMeanVertexLatency(vertexLatencySum / activeVertices);
			groupVertexSummary.setMeanVertexLatencyCV(vertexLatencyCASum / activeVertices);
		}
	}

	private void summarizeGroupEdge(SequenceElement seqElem,
			QosGroupEdgeSummary groupEdgeSummary, long inactivityThresholdTime) {

		int activeEdges = 0;
		double outputBufferLatencySum = 0;
		double transportLatencySum = 0;

		int activeConsumerVertices = 0;
		double consumptionRateSum = 0;
		double interarrivalTimeSum = 0;
		double interarrivalTimeCASum = 0;

		int inputGateIndex = seqElem.getInputGateIndex();
		QosGroupVertex targetGroupVertex = qosGraph.getGroupVertexByID(seqElem.getTargetVertexID());

		for (QosVertex memberVertex : targetGroupVertex.getMembers()) {
			VertexQosData qosData = memberVertex.getQosData();

			if (qosData.hasNewerData(inputGateIndex, -1, inactivityThresholdTime)) {
				activeConsumerVertices++;
				consumptionRateSum += qosData.getRecordsConsumedPerSec(inputGateIndex);
				interarrivalTimeSum += qosData.getInterArrivalTimeInMillis(inputGateIndex);
				interarrivalTimeCASum += qosData.getInterArrivalTimeCV(inputGateIndex);
			}

			for (QosEdge ingoingEdge : memberVertex.getInputGate(inputGateIndex).getEdges()) {
				EdgeQosData edgeQosData = ingoingEdge.getQosData();

				if (edgeQosData.hasNewerData(inactivityThresholdTime)) {
					activeEdges++;
					outputBufferLatencySum += edgeQosData.estimateOutputBufferLatencyInMillis();
					transportLatencySum += edgeQosData.estimateTransportLatencyInMillis();
				}
			}
		}

		if (activeEdges > 0 && activeConsumerVertices > 0) {
			groupEdgeSummary.setActiveEdges(activeEdges);
			groupEdgeSummary.setOutputBufferLatencyMean(outputBufferLatencySum / activeEdges);
			groupEdgeSummary.setTransportLatencyMean(transportLatencySum / activeEdges);

			groupEdgeSummary.setActiveConsumerVertices(activeConsumerVertices);
			groupEdgeSummary.setMeanConsumptionRate(consumptionRateSum / activeConsumerVertices);
			groupEdgeSummary
				.setMeanConsumerVertexInterarrivalTime(interarrivalTimeSum / activeConsumerVertices);
			groupEdgeSummary
				.setMeanConsumerVertexInterarrivalTimeCV(interarrivalTimeCASum / activeConsumerVertices);
			setSourceGroupVertexEmissionRate(seqElem, inactivityThresholdTime, groupEdgeSummary);
		}
	}

	private void setSourceGroupVertexEmissionRate(SequenceElement seqElem,
			long inactivityThresholdTime, QosGroupEdgeSummary groupEdgeSummary) {

		int activeEmitterVertices = 0;
		double emissionRateSum = 0;

		int outputGateIndex = seqElem.getOutputGateIndex();
		QosGroupVertex sourceGroupVertex = qosGraph.getGroupVertexByID(seqElem.getSourceVertexID());

		for (QosVertex memberVertex : sourceGroupVertex.getMembers()) {
			VertexQosData qosData = memberVertex.getQosData();

			if (qosData.hasNewerData(-1, outputGateIndex, inactivityThresholdTime)) {
				activeEmitterVertices++;
				emissionRateSum += qosData.getRecordsEmittedPerSec(outputGateIndex);
			}
		}

		if (activeEmitterVertices > 0) {
			groupEdgeSummary.setActiveEmitterVertices(activeEmitterVertices);
			groupEdgeSummary.setMeanEmissionRate(emissionRateSum / activeEmitterVertices);
		}
	}

	public JobGraphLatencyConstraint getJobGraphLatencyConstraint(LatencyConstraintID constraintID) {
		return this.qosGraph.getConstraintByID(constraintID);
	}
	
	public Collection<JobGraphLatencyConstraint> getJobGraphLatencyConstraints() {
		return this.qosGraph.getConstraints();
	}

	public State getState() {
		return this.state;
	}
}