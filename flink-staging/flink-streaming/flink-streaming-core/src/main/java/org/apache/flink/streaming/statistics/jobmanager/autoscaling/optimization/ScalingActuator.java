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

package org.apache.flink.streaming.statistics.jobmanager.autoscaling.optimization;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * This class essentially wraps a queue of scaling actions and a thread that executes
 * the scaling actions. Scale-Ups are executed in batch, while scale-downs are executed one-by-one.
 * Via {@link #recomputeScalingActions()}, this class offers the implicit possibility to
 * cancel previously enqueued scaling actions. This is useful when you are doing a lot of scale-downs
 * (which may take a long time to complete), but halfway through the scale-down you decide that you
 * have to scale-up again (e.g. because input rate has changed).
 * <p/>
 * Created by Bjoern Lohrmann on 12/14/14.
 */
public class ScalingActuator {

	private static final Log LOG = LogFactory.getLog(ScalingActuator.class);

	private class ScalingAction {
		final JobVertexID vertexId;
		final int action;

		private ScalingAction(JobVertexID vertexID, int action) {
			this.vertexId = vertexID;
			this.action = action;
		}
	}

	private final Queue<ScalingAction> actionQueue = new LinkedList<ScalingAction>();

	private final ExecutorService executor = Executors.newSingleThreadExecutor();

	private final Map<JobVertexID, Integer> currentParallelism = new HashMap<JobVertexID, Integer>();

	private final Map<JobVertexID, Integer> targetParallelism = new HashMap<JobVertexID, Integer>();

	private final Map<JobVertexID, Integer> vertexTopologicalScores;

	private final Future<?> scalingExecutorFuture;

	private volatile CountDownLatch cooldownLatch;

	public ScalingActuator(ExecutionGraph execGraph, Map<JobVertexID, Integer> vertexTopologicalScores) {
		this.vertexTopologicalScores = vertexTopologicalScores;
		this.cooldownLatch = new CountDownLatch(8);

		fillCurrentParallelism(execGraph);

		scalingExecutorFuture = this.executor.submit(new Runnable() {
			@Override
			public void run() {
				try {
					while (!scalingExecutorFuture.isCancelled()) {
						cooldownLatch.await();
						int newCooldown = executeQueuedScalingActions();
						cooldownLatch = new CountDownLatch(newCooldown);
					}
				} catch (InterruptedException e) {
					// do nothing
				} catch (Exception e) {
					LOG.error("Error during scaling action", e);
				}
			}
		});
	}

	private void fillCurrentParallelism(ExecutionGraph execGraph) {
		for (ExecutionJobVertex groupVertex : execGraph.getAllVertices().values()) {
			int currParallelism;

			if (groupVertex.hasElasticNumberOfRunningSubtasks()) {
				currParallelism = groupVertex.getInitialElasticNumberOfRunningSubtasks();
			} else {
				currParallelism = groupVertex.getParallelism();
			}

			currentParallelism.put(groupVertex.getJobVertexId(), currParallelism);
		}
	}

	public synchronized void updateScalingActions(Map<JobVertexID, Integer> newParallelism) {
		targetParallelism.putAll(newParallelism);
		recomputeScalingActions();
		cooldownLatch.countDown();
	}

	private void recomputeScalingActions() {
		// enqueue scaling actions in topological order
		final JobVertexID[] topoSortedVertexIds = targetParallelism.keySet().toArray(
						new JobVertexID[targetParallelism.size()]);
		Arrays.sort(topoSortedVertexIds, new Comparator<JobVertexID>() {
			@Override
			public int compare(JobVertexID first, JobVertexID second) {
				int firstTopoScore = vertexTopologicalScores.get(first);
				int secondTopoScore = vertexTopologicalScores.get(second);
				return Integer.compare(firstTopoScore, secondTopoScore);
			}
		});

		// refill action queue with topologically sorted scaling actions.
		// scale-ups have priority over scale-downs
		actionQueue.clear();
		for (JobVertexID id : topoSortedVertexIds) {
			int diff = targetParallelism.get(id) - currentParallelism.get(id);
			if (diff > 0) {
				actionQueue.add(new ScalingAction(id, diff));
			}
		}

		for (JobVertexID id : topoSortedVertexIds) {
			int diff = targetParallelism.get(id) - currentParallelism.get(id);
			if (diff < 0) {
				for (int i = diff; i < 0; i++) {
					actionQueue.add(new ScalingAction(id, -1));
				}
			}
		}
		this.notifyAll();
	}

	private ScalingAction dequeueNextScalingAction(int pendingCooldown) throws InterruptedException {
		if (scalingExecutorFuture.isCancelled()) {
			return null;
		}

		ScalingAction action = null;

		synchronized (this) {
			while (action == null) {
				action = actionQueue.poll();

				if (action != null) {
					int currParallelism = currentParallelism.get(action.vertexId);
					currentParallelism.put(action.vertexId, currParallelism + action.action);
				} else {
					if (pendingCooldown == 0) {
						this.wait();
					} else {
						break;
					}
				}
			}
		}

		return action;
	}

	private int executeQueuedScalingActions() throws Exception {
		int pendingCooldown = 0;

		// TODO
//		ScalingAction action;
//		while ((action = dequeueNextScalingAction(pendingCooldown)) != null) {
//			if (action.action > 0) {
//				jm.scaleUpElasticTask(jobId, action.vertexId,
//								action.action);
//				pendingCooldown = 2;
//			} else {
//				jm.scaleDownElasticTask(jobId, action.vertexId,
//								-action.action);
//			}
//		}

		return pendingCooldown;
	}

	public void shutdown() {
		scalingExecutorFuture.cancel(false);

		executor.shutdown();
		try {
			executor.awaitTermination(1, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			executor.shutdownNow();
		}
	}
}
