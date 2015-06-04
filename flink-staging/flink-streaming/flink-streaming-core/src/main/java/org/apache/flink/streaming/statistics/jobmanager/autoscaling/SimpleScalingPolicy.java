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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.streaming.statistics.JobGraphLatencyConstraint;
import org.apache.flink.streaming.statistics.LatencyConstraintID;
import org.apache.flink.streaming.statistics.SequenceElement;
import org.apache.flink.streaming.statistics.jobmanager.autoscaling.optimization.GG1Server;
import org.apache.flink.streaming.statistics.jobmanager.autoscaling.optimization.GG1ServerKingman;
import org.apache.flink.streaming.statistics.jobmanager.autoscaling.optimization.Rebalancer;
import org.apache.flink.streaming.statistics.taskmanager.qosmanager.QosConstraintSummary;
import org.apache.flink.streaming.statistics.taskmanager.qosmanager.QosGroupEdgeSummary;
import org.apache.flink.streaming.statistics.taskmanager.qosmanager.QosGroupVertexSummary;
import org.apache.flink.streaming.statistics.taskmanager.qosmanager.QosUtils;
import org.apache.flink.streaming.statistics.util.QosStatisticsConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Scaling policy that scales individual group vertices based on their CPU load
 * and record sent-consumed ratios.
 *
 * @author Bjoern Lohrmann
 */
public class SimpleScalingPolicy extends AbstractScalingPolicy {

	private static final Log LOG = LogFactory.getLog(SimpleScalingPolicy.class);

	public SimpleScalingPolicy(
					ExecutionGraph execGraph,
					HashMap<LatencyConstraintID, JobGraphLatencyConstraint> qosConstraints) {
		super(execGraph, qosConstraints);
	}

	protected void getParallelismChangesForConstraint(JobGraphLatencyConstraint constraint,
	                                                  QosConstraintSummary constraintSummary,
	                                                  Map<JobVertexID, Integer> globalParallelismChanges)
					throws UnexpectedVertexExecutionStateException {

		ArrayList<GG1Server> servers = createServers(constraint, constraintSummary, globalParallelismChanges);

		Map<JobVertexID, Integer> localParallelismChanges;
		if (hasBottleneck(servers)) {
			localParallelismChanges = resolveBottleneck(servers);
		} else {
			localParallelismChanges = rebalance(constraint, constraintSummary, servers);
		}

		// merge with local with global parallelism changes
		mergeLocalParallelismChangesIntoGlobal(globalParallelismChanges, servers, localParallelismChanges);
	}

	private void mergeLocalParallelismChangesIntoGlobal(Map<JobVertexID, Integer> globalParallelismChanges,
					ArrayList<GG1Server> servers, Map<JobVertexID,
					Integer> localParallelismChanges) {

		for (GG1Server server : servers) {
			JobVertexID id = server.getGroupVertexID();

			if (!localParallelismChanges.containsKey(id)) {
				continue;
			}

			int newP = localParallelismChanges.get(id);
			if (globalParallelismChanges.containsKey(id)) {
				newP = Math.max(newP, globalParallelismChanges.get(id));
			}
			globalParallelismChanges.put(id, newP);
		}
	}


	private Map<JobVertexID, Integer> rebalance(JobGraphLatencyConstraint constraint,
	                       QosConstraintSummary constraintSummary,
	                       ArrayList<GG1Server> servers) {

		double targetQueueingTimeMillis = computeTargetQueueTimeOfElasticServers(constraint,
						constraintSummary, servers);

		Rebalancer reb = new Rebalancer(filterNonElasticServers(servers), targetQueueingTimeMillis);
		boolean rebalanceSuccess = reb.computeRebalancedParallelism();

		Map<JobVertexID, Integer> rebActions = reb.getScalingActions();
		logAction("Rebalance",
						rebActions,
						servers,
						String.format("rebalanceSuccess: %s | targetQueueTime:%.2fms | projectedQueueTime: %.2fms",
							Boolean.toString(rebalanceSuccess),
							targetQueueingTimeMillis,
							reb.getRebalancedQueueWait() * 1000));


		Map<JobVertexID, Integer> newParallelism = new HashMap<JobVertexID, Integer>();
		for (GG1Server server : servers) {
			JobVertexID id = server.getGroupVertexID();

			if (rebActions.containsKey(id) && Math.abs(rebActions.get(id)) / ((double) server.getCurrentParallelism()) >= 0.04) {
				newParallelism.put(id, server.getCurrentParallelism() + rebActions.get(id));
			}
		}

		return newParallelism;
}


	private ArrayList<GG1Server> filterNonElasticServers(ArrayList<GG1Server> servers) {

		ArrayList<GG1Server> ret = new ArrayList<GG1Server>();
		for (GG1Server server : servers) {
			if (server.isElastic()) {
				ret.add(server);
			}
		}
		return ret;
	}

	private ArrayList<GG1Server> createServers(JobGraphLatencyConstraint constraint,
	                                           QosConstraintSummary constraintSummary,
	                                           Map<JobVertexID, Integer> parallelismChanges) {

		ArrayList<GG1Server> gg1Servers = new ArrayList<GG1Server>();

		for (SequenceElement seqElem : constraint.getSequence()) {
			if (!seqElem.isEdge()) {
				continue;
			}

			ExecutionJobVertex consumerGroupVertex = getExecutionGraph().getJobVertex(
					seqElem.getTargetVertexID());
			QosGroupEdgeSummary edgeSummary = constraintSummary.getGroupEdgeSummary(seqElem.getIndexInSequence());
			JobVertexID id = consumerGroupVertex.getJobVertexId();

			int minParallelism;
			int maxParallelism;

			boolean isElastic = consumerGroupVertex.hasElasticNumberOfRunningSubtasks();
			if (isElastic) {
				minParallelism = consumerGroupVertex.getMinElasticNumberOfRunningSubtasks();

				if (parallelismChanges.get(id) != null) {
					minParallelism = Math.max(minParallelism, parallelismChanges.get(id));
				}

				maxParallelism = consumerGroupVertex.getMaxElasticNumberOfRunningSubtasks();
			} else {
				minParallelism = maxParallelism = consumerGroupVertex.getNumberOfRunningSubstasks();
			}

			gg1Servers.add(new GG1ServerKingman(id, minParallelism, maxParallelism, edgeSummary));
		}

		return gg1Servers;
	}

	public double computeTargetQueueTimeOfElasticServers(JobGraphLatencyConstraint constraint,
	                                                     QosConstraintSummary constraintSummary,
	                                                     ArrayList<GG1Server> servers) {

		double availableShippingDelayMillis = constraint.getLatencyConstraintInMillis();
		double nonElasticShippingDelayMillis = 0;

		int serverIndex = 0;
		int elasticServersCount = 0;
		for (SequenceElement seqElem : constraint.getSequence()) {
			if (seqElem.isEdge()) {
				QosGroupEdgeSummary edgeSummary = constraintSummary.getGroupEdgeSummary(seqElem.getIndexInSequence());

				if (!servers.get(serverIndex).isElastic()) {
					nonElasticShippingDelayMillis += edgeSummary.getOutputBufferLatencyMean();
					nonElasticShippingDelayMillis += edgeSummary.getTransportLatencyMean();
				} else {
					elasticServersCount++;
				}

				serverIndex++;
			} else {
				QosGroupVertexSummary vertexSummary = constraintSummary
						.getGroupVertexSummary(seqElem.getIndexInSequence());
				availableShippingDelayMillis -= vertexSummary.getMeanVertexLatency();
			}
		}

		if (availableShippingDelayMillis < 0) {
			LOG.warn("Rebalance: Constraints is unenforcable (task latency too high)");
			// if availableShippingDelay is negative, we cannot enforce the
			// constraint by scaling. The only thing left to do is log a warning
			// and do graceful degradation. for graceful degradation we allow
			// some queueing delay at elastic tasks.

			// allow 1ms of queueing delay
			return elasticServersCount*2;

		} else {
			//LOG.warn("Rebalance: Could not enforce constraint by scaling due to non-elastic (or scaled-out) job vertices");
			return (availableShippingDelayMillis * 0.2 * 0.9) / constraint.getSequence().getNumberOfEdges();
		}

//		else {
//			// availableShippingDelay right now is the time available for output
//			// buffer latency AND queue waiting before elastic tasks. According
//			// to the 80:20 rule, output buffer latency will adapt itself
//			// to be 80% of that, so we can take the remaining 20% for queueing
//			// (minus another 10% margin of safety).
//			return (availableShippingDelayMillis - nonElasticShippingDelayMillis) * 0.2 * 0.9;
//		}
	}

	/**
	 * This is a last resort technique, when a bottleneck has formed. With
	 * bottlenecks, queueing models are not applicable anymore. Resolve
	 * bottlenecks at least doubles the bottlneck's degree of parallelism, in
	 * the hope of resolving the bottleneck as fast as possible.
	 *
	 * @param servers            List of G/G/1 servers
	 */
	private Map<JobVertexID, Integer> resolveBottleneck(ArrayList<GG1Server> servers) {

		Map<JobVertexID, Integer> newParallelism = new HashMap<JobVertexID, Integer>();
		Map<JobVertexID, Integer> actions = new HashMap<JobVertexID, Integer>();

		for (GG1Server bottleneckServer : findBottleneckServers(servers)) {
			int currP = bottleneckServer.getCurrentParallelism();
			int doubleP = 2 * currP;
			int halfUtilizationP = (int) Math.ceil(2 * currP * bottleneckServer.getCurrentMeanUtilization());

			int maxP = bottleneckServer.getUpperBoundParallelism();

			int newP = Math.min(maxP, Math.max(doubleP, halfUtilizationP));

			if (newP > currP) {
				JobVertexID id = bottleneckServer.getGroupVertexID();
				actions.put(id, newP - currP);
				newParallelism.put(id, newP);
			} else {
				LOG.warn("ResolveBottleneck: Could not resolve bottleneck by scaling out, due to non-elastic (or already 100% scaled-out) job vertices");
			}
		}

		logAction("ResolveBottleneck", actions, servers, null);
		return newParallelism;
	}

	private void logAction(String operation, Map<JobVertexID, Integer> actions, ArrayList<GG1Server> servers, String msg) {
		// print some debug output
		StringBuilder strBuild = new StringBuilder();
		strBuild.append(String.format("%d %s: ", QosUtils.alignToInterval(
						System.currentTimeMillis(),
						QosStatisticsConfig.getAdjustmentIntervalMillis()) / 1000,
						operation));


		for(GG1Server server :servers) {
			if (actions.containsKey(server.getGroupVertexID())) {
				int action = actions.get(server.getGroupVertexID());
				strBuild.append(String.format("%d(%d)",
								server.getCurrentParallelism() + action,
								action));
			}
		}

		if(msg != null) {
			strBuild.append(" | ");
			strBuild.append(msg);
		}
		LOG.debug(strBuild.toString());
	}

	private boolean hasBottleneck(ArrayList<GG1Server> servers) {
		return !findBottleneckServers(servers).isEmpty();
	}

	private ArrayList<GG1Server> findBottleneckServers(ArrayList<GG1Server> servers) {
		final double bottleneckUtilizationThreshold = 0.99;

		ArrayList<GG1Server> bottlenecks = new ArrayList<GG1Server>();

		for (GG1Server server : servers) {
			if (server.getCurrentMeanUtilization() >= bottleneckUtilizationThreshold) {
				bottlenecks.add(server);
			}
		}

		return bottlenecks;
	}
}
