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

import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.streaming.statistics.JobGraphLatencyConstraint;
import org.apache.flink.streaming.statistics.LatencyConstraintID;
import org.apache.flink.streaming.statistics.taskmanager.qosmanager.QosConstraintSummary;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Abstract superclass of scaling policies providing commonly used
 * functionality.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public abstract class AbstractScalingPolicy {

	private final ExecutionGraph execGraph;

	private final HashMap<LatencyConstraintID, JobGraphLatencyConstraint> qosConstraints;

	public AbstractScalingPolicy(
			ExecutionGraph execGraph,
			HashMap<LatencyConstraintID, JobGraphLatencyConstraint> qosConstraints) {

		this.execGraph = execGraph;
		this.qosConstraints = qosConstraints;
	}

	public Map<JobVertexID, Integer> getParallelismChanges(List<QosConstraintSummary> constraintSummaries)
			throws UnexpectedVertexExecutionStateException {

		Map<JobVertexID, Integer> parallelismChanges = new HashMap<JobVertexID, Integer>();

		for (QosConstraintSummary constraintSummary : constraintSummaries) {
			if (constraintSummary.hasData()) {
				getParallelismChangesForConstraint(qosConstraints.get(constraintSummary.getLatencyConstraintID()), constraintSummary, parallelismChanges);
			}
		}

		return parallelismChanges;
	}

	protected abstract void getParallelismChangesForConstraint(
			JobGraphLatencyConstraint constraint,
			QosConstraintSummary constraintSummary,
			Map<JobVertexID, Integer> scalingActions)
			throws UnexpectedVertexExecutionStateException;

	protected ExecutionGraph getExecutionGraph() {
		return execGraph;
	}

	protected Map<LatencyConstraintID, JobGraphLatencyConstraint> getConstraints() {
		return qosConstraints;
	}
}
