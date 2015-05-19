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

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.streaming.statistics.JobGraphLatencyConstraint;
import org.apache.flink.streaming.statistics.SequenceElement;
import org.apache.flink.streaming.statistics.message.TaskCpuLoadChange;

import java.util.Map;

public class LatencyConstraintCpuLoadSummaryAggregator {

	private final ExecutionGraph execGraph;

	private final JobGraphLatencyConstraint qosConstraint;

	public LatencyConstraintCpuLoadSummaryAggregator(ExecutionGraph execGraph, JobGraphLatencyConstraint qosConstraint) {
		this.execGraph = execGraph;
		this.qosConstraint = qosConstraint;
	}

	public LatencyConstraintCpuLoadSummary summarizeCpuUtilizations(Map<ExecutionAttemptID, TaskCpuLoadChange> taskCpuLoads)
			throws UnexpectedVertexExecutionStateException {

		LatencyConstraintCpuLoadSummary summary = new LatencyConstraintCpuLoadSummary();

		for (SequenceElement seqElem : qosConstraint.getSequence()) {
			if (seqElem.isEdge()) {

				JobVertexID sourceVertexID = seqElem.getSourceVertexID();
				if (!summary.containsKey(sourceVertexID)) {
					summary.put(sourceVertexID,
							new GroupVertexCpuLoadSummary(taskCpuLoads, execGraph.getJobVertex(sourceVertexID)));
				}

				JobVertexID targetVertexID = seqElem.getTargetVertexID();
				if (!summary.containsKey(targetVertexID)) {
					summary.put(targetVertexID,
							new GroupVertexCpuLoadSummary(taskCpuLoads, execGraph.getJobVertex(targetVertexID)));
				}

			}
		}

		return summary;
	}
}
