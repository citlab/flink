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

package org.apache.flink.streaming.statistics.jobmanager.autoscaling;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.streaming.statistics.JobGraphLatencyConstraint;
import org.apache.flink.streaming.statistics.LatencyConstraintID;
import org.apache.flink.streaming.statistics.jobmanager.autoscaling.optimization.ScalingActuator;
import org.apache.flink.streaming.statistics.message.AbstractQosMessage;
import org.apache.flink.streaming.statistics.taskmanager.qosmanager.QosConstraintSummary;
import org.apache.flink.streaming.statistics.taskmanager.qosmanager.QosLogger;
import org.apache.flink.streaming.statistics.taskmanager.qosmanager.QosUtils;
import org.apache.flink.streaming.statistics.taskmanager.qosmodel.QosGraph;
import org.apache.flink.streaming.statistics.taskmanager.qosmodel.QosGroupEdge;
import org.apache.flink.streaming.statistics.taskmanager.qosmodel.QosGroupVertex;
import org.apache.flink.streaming.statistics.util.QosStatisticsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

// TODO: add web statistics
public class ElasticTaskQosAutoScalingThread extends Thread {

	private static final Logger LOG = LoggerFactory.getLogger(ElasticTaskQosAutoScalingThread.class);

	private final LinkedBlockingQueue<AbstractQosMessage> qosMessages = new LinkedBlockingQueue<AbstractQosMessage>();

	private long timeOfLastScaling;

	private long timeOfNextScaling;

	private final HashMap<LatencyConstraintID, QosConstraintSummaryAggregator> aggregators = new HashMap<LatencyConstraintID, QosConstraintSummaryAggregator>();

	private final HashMap<LatencyConstraintID, QosLogger> qosLoggers = new HashMap<LatencyConstraintID, QosLogger>();

	private final ScalingActuator scalingActuator;

//	private final QosJobWebStatistic webStatistic;

	private AbstractScalingPolicy scalingPolicy;

	public ElasticTaskQosAutoScalingThread(ExecutionGraph execGraph, QosGraph qosGraph) {
		this.setName("QosAutoScalingThread");
		this.timeOfLastScaling = 0;
		this.timeOfNextScaling = 0;

		JobID jobId = execGraph.getJobID();
		long loggingInterval = QosStatisticsConfig.getAdjustmentIntervalMillis();
		HashMap<LatencyConstraintID, JobGraphLatencyConstraint> qosConstraints = new HashMap<LatencyConstraintID, JobGraphLatencyConstraint>();

		for (JobGraphLatencyConstraint constraint : qosGraph.getConstraints()) {

			LatencyConstraintID constraintID = constraint.getID();

			qosConstraints.put(constraintID, constraint);
			aggregators.put(constraintID, new QosConstraintSummaryAggregator(execGraph, constraint));

			try {
				qosLoggers.put(constraintID, new QosLogger(jobId, constraint, loggingInterval));
			} catch (Exception e) {
				LOG.error("Exception while initializing loggers", e);
			}
		}

		scalingPolicy = new SimpleScalingPolicy(execGraph, qosConstraints);
		scalingActuator = new ScalingActuator(execGraph, getVertexTopologicalScores(qosGraph));

//		webStatistic = new QosJobWebStatistic(execGraph, loggingInterval, qosConstraints);
//		QosStatisticsServlet.putStatistic(this.jobID, webStatistic);

//		this.start();
	}

	private HashMap<JobVertexID, Integer> getVertexTopologicalScores(QosGraph qosGraph) {

		Map<JobVertexID, Integer> predecessorCounts = new HashMap<JobVertexID, Integer>();
		LinkedList<JobVertexID> verticesWithoutPredecessor = new LinkedList<JobVertexID>();

		for (QosGroupVertex groupVertex : qosGraph.getAllVertices()) {
			int noOfPredecessors = groupVertex.getNumberOfInputGates();
			predecessorCounts.put(groupVertex.getJobVertexID(),
					noOfPredecessors);

			if (noOfPredecessors == 0) {
				verticesWithoutPredecessor.add(groupVertex.getJobVertexID());
			}
		}

		HashMap<JobVertexID, Integer> vertexTopologicalScores = new HashMap<JobVertexID, Integer>();

		int nextTopoScore = 0;
		while (!verticesWithoutPredecessor.isEmpty()) {
			JobVertexID vertexWithoutPredecessor = verticesWithoutPredecessor.removeFirst();

			vertexTopologicalScores.put(vertexWithoutPredecessor, nextTopoScore);
			nextTopoScore++;

			for (QosGroupEdge forwardEdge : qosGraph.getGroupVertexByID(
					vertexWithoutPredecessor).getForwardEdges()) {
				QosGroupVertex successor = forwardEdge.getTargetVertex();

				int newPredecessorCount = predecessorCounts.get(successor.getJobVertexID()) - 1;
				predecessorCounts.put(successor.getJobVertexID(), newPredecessorCount);

				if (newPredecessorCount == 0) {
					verticesWithoutPredecessor.add(successor.getJobVertexID());
				}
			}
		}

		return vertexTopologicalScores;
	}

	@Override
	public void run() {
		try {
			LOG.info("Qos Auto Scaling Thread started");

			long now;

			while (!interrupted()) {
				processMessages();
				Thread.sleep(500);

				now = System.currentTimeMillis();

				if (scalingIsDue(now)) {
					List<QosConstraintSummary> constraintSummaries = aggregateConstraintSummaries();
					logConstraintSummaries(constraintSummaries);

					Map<JobVertexID, Integer> parallelismChanges = scalingPolicy.getParallelismChanges(constraintSummaries);
					scalingActuator.updateScalingActions(parallelismChanges);

					LOG.debug(String.format("%d %s", QosUtils.alignToInterval(
													System.currentTimeMillis(),
													QosStatisticsConfig.getAdjustmentIntervalMillis()) / 1000,
													parallelismChanges.toString()));

					timeOfLastScaling = System.currentTimeMillis();
					timeOfNextScaling = timeOfLastScaling + QosStatisticsConfig.getAdjustmentIntervalMillis();
				}
			}
		} catch (InterruptedException e) {
			// do nothing
		} catch (UnexpectedVertexExecutionStateException e) {
			// do nothing, the job is usually finishing/canceling/failing
		} catch (Exception e) {
			LOG.error("Exception in auto scaling thread", e);
		} finally {
			cleanUp();
		}

		LOG.info("Qos Auto Scaling Thread stopped.");
	}


	private List<QosConstraintSummary> aggregateConstraintSummaries() {
		LinkedList<QosConstraintSummary> toReturn = new LinkedList<QosConstraintSummary>();

		for (QosConstraintSummaryAggregator aggregator : aggregators.values()) {
			toReturn.add(aggregator.computeAggregation());
		}

		return toReturn;
	}

	private void logConstraintSummaries(
			List<QosConstraintSummary> constraintSummaries) {

		for (QosConstraintSummary constraintSummary : constraintSummaries) {
			QosLogger logger = qosLoggers.get(constraintSummary
					.getLatencyConstraintID());

			if (logger != null) {
				try {
					logger.logSummary(constraintSummary);

				} catch (IOException e) {
					LOG.error("Error during QoS logging", e);
				}
			}
		}

//		webStatistic.logConstraintSummaries(constraintSummaries);
	}

	private void cleanUp() {
		for (QosLogger logger : qosLoggers.values()) {
			try {
				logger.close();
			} catch (IOException e) {
				LOG.warn("Failure while closing qos logger!", e);
			}
		}

		// clear large memory structures
		qosMessages.clear();
		aggregators.clear();
		scalingPolicy = null;
		scalingActuator.shutdown();
//		QosStatisticsServlet.removeJob(this.jobID);
	}

	private boolean scalingIsDue(long now) {
		if (now < timeOfNextScaling) {
			return false;
		}

		for (QosConstraintSummaryAggregator summaryAggregator : aggregators.values()) {
			if (!summaryAggregator.canAggregate()) {
				return false;
			}
		}

		return true;
	}

	private void processMessages() {
		while (!qosMessages.isEmpty()) {
			AbstractQosMessage nextMessage = qosMessages.poll();

			if (nextMessage instanceof  QosConstraintSummary) {
				QosConstraintSummary constraintSummary = (QosConstraintSummary) nextMessage;
				LatencyConstraintID constraintID = constraintSummary.getLatencyConstraintID();
				aggregators.get(constraintID).add(constraintSummary);
			}
		}
	}

	public void enqueueMessage(QosConstraintSummary summary) {
		this.qosMessages.add(summary);
	}

	public void shutdown() {
		this.interrupt();
	}
}
