/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.graph;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.AbstractJobVertex;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.streaming.api.constraint.ConstraintBoundary;
import org.apache.flink.streaming.api.constraint.ConstraintGroupConfiguration;
import org.apache.flink.streaming.api.constraint.JobGraphConstraintGroup;
import org.apache.flink.streaming.api.constraint.JobGraphSequenceElement;
import org.apache.flink.streaming.api.constraint.JobGraphSequenceFinder;
import org.apache.flink.streaming.api.constraint.identifier.ConstraintGroupIdentifier;
import org.apache.flink.streaming.api.graph.StreamGraph.StreamLoop;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator.ChainingStrategy;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner.PartitioningStrategy;
import org.apache.flink.streaming.runtime.tasks.StreamIterationHead;
import org.apache.flink.streaming.runtime.tasks.StreamIterationTail;
import org.apache.flink.streaming.statistics.CentralQosStatisticsHandler;
import org.apache.flink.streaming.statistics.ConstraintUtil;
import org.apache.flink.streaming.statistics.JobGraphSequence;
import org.apache.flink.streaming.statistics.SequenceElement;
import org.apache.flink.streaming.statistics.message.action.EdgeQosReporterConfig;
import org.apache.flink.streaming.statistics.message.action.VertexQosReporterConfig;
import org.apache.flink.streaming.statistics.taskmanager.qosreporter.QosReportForwarderThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class StreamingJobGraphGenerator {

	private static final Logger LOG = LoggerFactory.getLogger(StreamingJobGraphGenerator.class);

	private StreamGraph streamGraph;

	private Map<Integer, AbstractJobVertex> jobVertices;
	private JobGraph jobGraph;
	private Collection<Integer> builtVertices;

	private List<StreamEdge> physicalEdgesInOrder;

	private Map<Integer, Map<Integer, StreamConfig>> chainedConfigs;

	private Map<Integer, StreamConfig> vertexConfigs;
	private Map<Integer, String> chainedNames;

	public StreamingJobGraphGenerator(StreamGraph streamGraph) {
		this.streamGraph = streamGraph;
	}

	private void init() {
		this.jobVertices = new HashMap<Integer, AbstractJobVertex>();
		this.builtVertices = new HashSet<Integer>();
		this.chainedConfigs = new HashMap<Integer, Map<Integer, StreamConfig>>();
		this.vertexConfigs = new HashMap<Integer, StreamConfig>();
		this.chainedNames = new HashMap<Integer, String>();
		this.physicalEdgesInOrder = new ArrayList<StreamEdge>();
	}

	public JobGraph createJobGraph(String jobName) {
		jobGraph = new JobGraph(jobName);

		// Turn lazy scheduling off
		jobGraph.setScheduleMode(ScheduleMode.ALL);
		jobGraph.setJobType(JobGraph.JobType.STREAMING);
		jobGraph.setCheckpointingEnabled(streamGraph.isCheckpointingEnabled());
		jobGraph.setCheckpointingInterval(streamGraph.getCheckpointingInterval());

		if (jobGraph.isCheckpointingEnabled()) {
			int executionRetries = streamGraph.getExecutionConfig().getNumberOfExecutionRetries();
			if (executionRetries != -1) {
				jobGraph.setNumberOfExecutionRetries(executionRetries);
			} else {
				jobGraph.setNumberOfExecutionRetries(Integer.MAX_VALUE);
			}
		}
		init();

		setChaining();

		setPhysicalEdges();

		setSlotSharing();

		if (streamGraph.hasLatencyConstraints()) {
			setLatencyConstraints();
		}

		return jobGraph;
	}

	private void setPhysicalEdges() {
		Map<Integer, List<StreamEdge>> physicalInEdgesInOrder = new HashMap<Integer, List<StreamEdge>>();

		for (StreamEdge edge : physicalEdgesInOrder) {
			int target = edge.getTargetID();

			List<StreamEdge> inEdges = physicalInEdgesInOrder.get(target);

			// create if not set
			if (inEdges == null) {
				inEdges = new ArrayList<StreamEdge>();
				physicalInEdgesInOrder.put(target, inEdges);
			}

			inEdges.add(edge);
		}

		for (Map.Entry<Integer, List<StreamEdge>> inEdges : physicalInEdgesInOrder.entrySet()) {
			int vertex = inEdges.getKey();
			List<StreamEdge> edgeList = inEdges.getValue();

			vertexConfigs.get(vertex).setInPhysicalEdges(edgeList);
		}
	}

	private void setChaining() {
		if (streamGraph.hasLatencyConstraints() && streamGraph.isChainingEnabled()) {
			// prevent chaining over constraint boundaries (if desired)
			for (ConstraintGroupConfiguration conf : streamGraph.getConstraintConfigurations().values()) {
				if (conf.isDisableChaining()) {
					adjustChaining(conf.getStart().getTargetId());
					adjustChaining(conf.getEnd().getTargetId());
				}
			}
		}

		for (Integer sourceName : streamGraph.getSourceIDs()) {
			createChain(sourceName, sourceName);
		}
	}

	private void adjustChaining(int vertexId) {
		StreamOperator<?, ?> invokable = streamGraph.getVertex(vertexId).getOperator();
		if (invokable.getChainingStrategy() == ChainingStrategy.ALWAYS) {
			invokable.setChainingStrategy(ChainingStrategy.HEAD);
		}
	}

	private List<StreamEdge> createChain(Integer startNode, Integer current) {

		if (!builtVertices.contains(startNode)) {

			List<StreamEdge> transitiveOutEdges = new ArrayList<StreamEdge>();

			List<StreamEdge> chainableOutputs = new ArrayList<StreamEdge>();
			List<StreamEdge> nonChainableOutputs = new ArrayList<StreamEdge>();

			for (StreamEdge outEdge : streamGraph.getVertex(current).getOutEdges()) {
				if (isChainable(outEdge)) {
					chainableOutputs.add(outEdge);
				} else {
					nonChainableOutputs.add(outEdge);
				}
			}

			for (StreamEdge chainable : chainableOutputs) {
				transitiveOutEdges.addAll(createChain(startNode, chainable.getTargetID()));
			}

			for (StreamEdge nonChainable : nonChainableOutputs) {
				transitiveOutEdges.add(nonChainable);
				createChain(nonChainable.getTargetID(), nonChainable.getTargetID());
			}

			chainedNames.put(current, createChainedName(current, chainableOutputs));

			StreamConfig config = current.equals(startNode) ? createProcessingVertex(startNode)
					: new StreamConfig(new Configuration());

			setVertexConfig(current, config, chainableOutputs, nonChainableOutputs);

			if (current.equals(startNode)) {

				config.setChainStart();
				config.setOutEdgesInOrder(transitiveOutEdges);
				config.setOutEdges(streamGraph.getVertex(current).getOutEdges());

				for (StreamEdge edge : transitiveOutEdges) {
					connect(startNode, edge);
				}

				config.setTransitiveChainedTaskConfigs(chainedConfigs.get(startNode));

			} else {

				Map<Integer, StreamConfig> chainedConfs = chainedConfigs.get(startNode);

				if (chainedConfs == null) {
					chainedConfigs.put(startNode, new HashMap<Integer, StreamConfig>());
				}
				chainedConfigs.get(startNode).put(current, config);
			}

			return transitiveOutEdges;

		} else {
			return new ArrayList<StreamEdge>();
		}
	}

	private String createChainedName(Integer vertexID, List<StreamEdge> chainedOutputs) {
		String operatorName = streamGraph.getVertex(vertexID).getOperatorName();
		if (chainedOutputs.size() > 1) {
			List<String> outputChainedNames = new ArrayList<String>();
			for (StreamEdge chainable : chainedOutputs) {
				outputChainedNames.add(chainedNames.get(chainable.getTargetID()));
			}
			String returnOperatorName = operatorName + " -> ("
					+ StringUtils.join(outputChainedNames, ", ") + ")";
			return returnOperatorName;
		} else if (chainedOutputs.size() == 1) {
			String returnOperatorName = operatorName + " -> "
					+ chainedNames.get(chainedOutputs.get(0).getTargetID());
			return returnOperatorName;
		} else {
			return operatorName;
		}

	}

	private StreamConfig createProcessingVertex(Integer vertexID) {

		AbstractJobVertex jobVertex = new AbstractJobVertex(chainedNames.get(vertexID));
		StreamNode vertex = streamGraph.getVertex(vertexID);

		jobVertex.setInvokableClass(vertex.getJobVertexClass());

		int parallelism = vertex.getParallelism();

		if (parallelism > 0) {
			jobVertex.setParallelism(parallelism);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Parallelism set: {} for {}", parallelism, vertexID);
		}

		if (vertex.getInputFormat() != null) {
			jobVertex.setInputSplitSource(vertex.getInputFormat());
		}

		jobVertices.put(vertexID, jobVertex);
		builtVertices.add(vertexID);
		jobGraph.addVertex(jobVertex);

		StreamConfig retConfig = new StreamConfig(jobVertex.getConfiguration());
		retConfig.setOperatorName(chainedNames.get(vertexID));
		return retConfig;
	}

	private void setVertexConfig(Integer vertexID, StreamConfig config,
			List<StreamEdge> chainableOutputs, List<StreamEdge> nonChainableOutputs) {

		StreamNode vertex = streamGraph.getVertex(vertexID);

		config.setVertexID(vertexID);
		config.setBufferTimeout(vertex.getBufferTimeout());

		config.setTypeSerializerIn1(vertex.getTypeSerializerIn1());
		config.setTypeSerializerIn2(vertex.getTypeSerializerIn2());
		config.setTypeSerializerOut1(vertex.getTypeSerializerOut());

		config.setStreamOperator(vertex.getOperator());
		config.setOutputSelectorWrapper(vertex.getOutputSelectorWrapper());

		config.setNumberOfOutputs(nonChainableOutputs.size());
		config.setNonChainedOutputs(nonChainableOutputs);
		config.setChainedOutputs(chainableOutputs);
		config.setStateMonitoring(streamGraph.isCheckpointingEnabled());

		Class<? extends AbstractInvokable> vertexClass = vertex.getJobVertexClass();

		if (vertexClass.equals(StreamIterationHead.class)
				|| vertexClass.equals(StreamIterationTail.class)) {
			config.setIterationId(streamGraph.getLoopID(vertexID));
			config.setIterationWaitTime(streamGraph.getLoopTimeout(vertexID));
		}

		List<StreamEdge> allOutputs = new ArrayList<StreamEdge>(chainableOutputs);
		allOutputs.addAll(nonChainableOutputs);

		for (StreamEdge output : allOutputs) {
			config.setSelectedNames(output.getTargetID(),
					streamGraph.getEdge(vertexID, output.getTargetID()).getSelectedNames());
		}

		vertexConfigs.put(vertexID, config);
	}

	private void connect(Integer headOfChain, StreamEdge edge) {

		physicalEdgesInOrder.add(edge);

		Integer downStreamvertexID = edge.getTargetID();

		AbstractJobVertex headVertex = jobVertices.get(headOfChain);
		AbstractJobVertex downStreamVertex = jobVertices.get(downStreamvertexID);

		StreamConfig downStreamConfig = new StreamConfig(downStreamVertex.getConfiguration());

		downStreamConfig.setNumberOfInputs(downStreamConfig.getNumberOfInputs() + 1);

		StreamPartitioner<?> partitioner = edge.getPartitioner();
		if (partitioner.getStrategy() == PartitioningStrategy.FORWARD) {
			downStreamVertex.connectNewDataSetAsInput(headVertex, DistributionPattern.POINTWISE);
		} else {
			downStreamVertex.connectNewDataSetAsInput(headVertex, DistributionPattern.ALL_TO_ALL);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("CONNECTED: {} - {} -> {}", partitioner.getClass().getSimpleName(),
					headOfChain, downStreamvertexID);
		}
	}

	private boolean isChainable(StreamEdge edge) {
		StreamNode upStreamVertex = edge.getSourceVertex();
		StreamNode downStreamVertex = edge.getTargetVertex();

		StreamOperator<?, ?> headOperator = upStreamVertex.getOperator();
		StreamOperator<?, ?> outOperator = downStreamVertex.getOperator();

		return downStreamVertex.getInEdges().size() == 1
				&& outOperator != null
				&& outOperator.getChainingStrategy() == ChainingStrategy.ALWAYS
				&& (headOperator.getChainingStrategy() == ChainingStrategy.HEAD || headOperator
						.getChainingStrategy() == ChainingStrategy.ALWAYS)
				&& (edge.getPartitioner().getStrategy() == PartitioningStrategy.FORWARD || downStreamVertex
						.getParallelism() == 1)
				&& upStreamVertex.getParallelism() == downStreamVertex.getParallelism()
				&& streamGraph.isChainingEnabled();
	}

	private void setSlotSharing() {
		SlotSharingGroup shareGroup = new SlotSharingGroup();

		for (AbstractJobVertex vertex : jobVertices.values()) {
			vertex.setSlotSharingGroup(shareGroup);
		}

		for (StreamLoop loop : streamGraph.getStreamLoops()) {
			CoLocationGroup ccg = new CoLocationGroup();
			AbstractJobVertex tail = jobVertices.get(loop.getTail().getID());
			AbstractJobVertex head = jobVertices.get(loop.getHead().getID());

			ccg.addVertex(head);
			ccg.addVertex(tail);
		}
	}

	private void setLatencyConstraints() {
		// calcuate constraints
		Map<ConstraintGroupIdentifier, JobGraphConstraintGroup> constraints =
				new HashMap<ConstraintGroupIdentifier, JobGraphConstraintGroup>();
		Map<ConstraintGroupIdentifier, ConstraintGroupConfiguration> confs = streamGraph.getConstraintConfigurations();


		for (Map.Entry<ConstraintGroupIdentifier, ConstraintGroupConfiguration> entry : confs.entrySet()) {
			ConstraintBoundary beginEdge = entry.getValue().getStart();
			ConstraintBoundary endEdge = entry.getValue().getEnd();

			AbstractJobVertex beginVertex = jobVertices.get(beginEdge.getTargetId());
			AbstractJobVertex endVertex = jobVertices.get(endEdge.getSourceId());

			if (beginVertex == null || endVertex == null) {
				throw new IllegalStateException("Chaining over latency constraint boundaries detected. " +
						"Please disable chaining over the constraint boundaries.");
			}

			JobVertexID beginId = beginVertex.getID();
			JobVertexID endId = endVertex.getID();

			JobGraphSequenceFinder jobGraphSequenceFinder = new JobGraphSequenceFinder(jobGraph);
			List<org.apache.flink.streaming.api.constraint.JobGraphSequence> sequences =
					jobGraphSequenceFinder.findAllSequencesBetween(beginId, endId);

			for (org.apache.flink.streaming.api.constraint.JobGraphSequence sequence : sequences) {
				// add first edge
				JobVertexID beginSource = jobVertices.get(beginEdge.getSourceId()).getID();
				sequence.addFirst(new JobGraphSequenceElement(beginSource, beginId, 0));

				// add last edge
				JobVertexID endTarget = jobVertices.get(endEdge.getTargetId()).getID();
				sequence.addLast(new JobGraphSequenceElement(endId, endTarget, 0));
			}

			JobGraphConstraintGroup constraintGroup = new JobGraphConstraintGroup(sequences,
					entry.getValue().getMaxLatency());
			constraints.put(entry.getKey(), constraintGroup);
		}


		setLatencyConstraints(constraints);
	}


	// TODO clean this up
	// some of this is actually obsolete, as the constraints are already calculated on job graph level
	private void setLatencyConstraints(Map<ConstraintGroupIdentifier, JobGraphConstraintGroup> constraints) {
		jobGraph.setCustomStatisticsEnabled(true);
		jobGraph.setCustomAbstractCentralStatisticsHandler(new CentralQosStatisticsHandler());
		// central statistics handler (job manager) report interval
		long reportInterval = streamGraph.getQosStatisticReportInterval();
		jobGraph.setCustomStatisticsInterval(reportInterval);
		// forwarder (task manager) report interval
		jobGraph.getJobConfiguration().setLong(QosReportForwarderThread.FORWARDER_REPORT_INTERVAL_KEY, reportInterval);


		for (Map.Entry<ConstraintGroupIdentifier, JobGraphConstraintGroup> entry : constraints.entrySet()) {
			ConstraintGroupIdentifier identifier = entry.getKey();
			JobGraphConstraintGroup constraintGroup = entry.getValue();

			for (org.apache.flink.streaming.api.constraint.JobGraphSequence sequence : constraintGroup.getSequences()) {
				JobGraphSequence seq = new JobGraphSequence();
				int lastInputGateIndex = -1;

				for (JobGraphSequenceElement element : sequence) {

					if (!element.isVertex()) {
						AbstractJobVertex sourceVertex = jobGraph.findVertexByID(element.getVertexId());
						AbstractJobVertex targetVertex = jobGraph.findVertexByID(element.getTargetId());
						List<IntermediateDataSet> producedDataSets = sourceVertex.getProducedDataSets();
						List<JobEdge> inputEdges = targetVertex.getInputs();

						JobEdge edge = null;
						for (JobEdge e : inputEdges) {
							if (producedDataSets.get(element.getTargetIndex()).getId().equals(e.getSourceId())) {
								edge = e;
								break;
							}
						}
						if (edge == null) {
							throw new RuntimeException("Error when calculating latency constraints. This is a bug.");
						}

						int outputGateIndex = producedDataSets.indexOf(edge.getSource());
						int inputGateIndex = inputEdges.indexOf(edge);

						if (element.getTargetIndex() != outputGateIndex) {
							throw new RuntimeException("Target and output gate index are not equal! This is Bug.");
						}

						// constraint always begin and end with a edge at the moment
						if (!(element == sequence.getFirst() || element == sequence.getLast())) {
							addVertexQosConfig(seq, sourceVertex, lastInputGateIndex, outputGateIndex);
						}
						addEdgeQosConfig(seq, edge.getSourceId(), sourceVertex, outputGateIndex, targetVertex, inputGateIndex);

						lastInputGateIndex = inputGateIndex;
					}
				}

				// persist constraints in job graph
				try {
					String name = identifier.toString() + ": " + sequence.toString();
					ConstraintUtil.defineLatencyConstraint(seq, constraintGroup.getMaxLatency(), jobGraph, name);
				} catch (IOException e) {
					throw new RuntimeException("LatencyConstraint serialization failed.");
				}
			}
		}
	}

	/**
	 * Adds vertex to given sequence and qos reporter config to stream (task) config.
	 */
	private void addVertexQosConfig(JobGraphSequence seq,
									AbstractJobVertex vertex, int inputGateIndex, int ouputGateIndex) {

		SequenceElement e = seq.addVertex(vertex.getID(), vertex.getName(), inputGateIndex, ouputGateIndex);
		VertexQosReporterConfig reporterConfig = VertexQosReporterConfig.fromSequenceElement(vertex, e);
		StreamConfig streamConfig = new StreamConfig(vertex.getConfiguration());
		streamConfig.addQosReporterConfigs(reporterConfig);
	}

	/**
	 * Adds edge to given sequence and qos reporter config to source and target stream (task) configs.
	 */
	private void addEdgeQosConfig(JobGraphSequence seq, IntermediateDataSetID dataSetID,
									AbstractJobVertex sourceVertex, int outputGateIndex,
									AbstractJobVertex targetVertex, int inputGateIndex) {

		seq.addEdge(sourceVertex.getID(), outputGateIndex, targetVertex.getID(), inputGateIndex);
		String name = String.format("%s -> %s", sourceVertex.getName(), targetVertex.getName());
		EdgeQosReporterConfig reporterConfig = new EdgeQosReporterConfig(dataSetID, outputGateIndex, inputGateIndex, name);
		StreamConfig sourceConfig = new StreamConfig(sourceVertex.getConfiguration());
		sourceConfig.addQosReporterConfigs(reporterConfig);
		StreamConfig targetConfig = new StreamConfig(targetVertex.getConfiguration());
		targetConfig.addQosReporterConfigs(reporterConfig);
	}
}
