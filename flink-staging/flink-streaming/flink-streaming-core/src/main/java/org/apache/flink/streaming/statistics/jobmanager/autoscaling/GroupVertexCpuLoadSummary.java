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

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.streaming.statistics.message.CpuLoadClassifier;
import org.apache.flink.streaming.statistics.message.CpuLoadClassifier.CpuLoad;
import org.apache.flink.streaming.statistics.message.TaskCpuLoadChange;

import java.util.Map;

public class GroupVertexCpuLoadSummary {

	private int lows = 0;

	private int mediums = 0;

	private int highs = 0;

	private int unknowns = 0;

	private double averageCpuUtilization = 0;

	public GroupVertexCpuLoadSummary(
			Map<ExecutionAttemptID, TaskCpuLoadChange> taskCpuLoads,
			ExecutionJobVertex groupVertex)
			throws UnexpectedVertexExecutionStateException {

		int noOfRunningTasks = groupVertex
				.getCurrentElasticNumberOfRunningSubtasks();

		for (int i = 0; i < noOfRunningTasks; i++) {
			ExecutionVertex execVertex = groupVertex.getTaskVertices()[i];
			ExecutionAttemptID attemptID = execVertex.getCurrentExecutionAttempt().getAttemptId();
			ExecutionState vertexState = execVertex.getExecutionState();

			switch (vertexState) {
			case RUNNING:
				if (taskCpuLoads.containsKey(attemptID)) {
					TaskCpuLoadChange taskLoad = taskCpuLoads.get(attemptID);

					averageCpuUtilization += taskLoad.getCpuUtilization();

					switch (taskLoad.getLoadState()) {
					case LOW:
						lows++;
						break;
					case MEDIUM:
						mediums++;
						break;
					case HIGH:
						highs++;
						break;
					}
				} else {
					unknowns++;
				}
				break;
			case SUSPENDING:
			case SUSPENDED:
				break;
			default:
				throw new UnexpectedVertexExecutionStateException();
			}
		}

		int aggregatedTasks = lows + mediums + highs;

		if (aggregatedTasks == 0) {
			throw new RuntimeException(
					"No running tasks with available CPU utilization data");
		} else {
			averageCpuUtilization /= aggregatedTasks;
		}
	}

	public int getLows() {
		return lows;
	}

	public int getMediums() {
		return mediums;
	}

	public int getHighs() {
		return highs;
	}

	public int getUnknowns() {
		return unknowns;
	}

	public double getAvgCpuUtilization() {
		return averageCpuUtilization;
	}
	
	public CpuLoad getAvgCpuLoad() {
		return CpuLoadClassifier.fromCpuUtilization(averageCpuUtilization);
	}
}
