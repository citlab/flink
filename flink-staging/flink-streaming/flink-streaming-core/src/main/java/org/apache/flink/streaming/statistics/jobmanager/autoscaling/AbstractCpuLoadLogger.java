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
import org.apache.flink.streaming.statistics.taskmanager.qosmanager.QosUtils;
import org.codehaus.jettison.json.JSONException;

import java.io.IOException;
import java.util.Map;

public abstract class AbstractCpuLoadLogger {
	protected final long loggingInterval;
	protected final JobVertexID groupVertices[];

	public AbstractCpuLoadLogger(ExecutionGraph execGraph, JobGraphLatencyConstraint constraint, long loggingInterval) {
		this.loggingInterval = loggingInterval;
		this.groupVertices = constraint.getSequence().getVerticesForSequenceOrdered(true).toArray(new JobVertexID[0]);
	}

	public abstract void logCpuLoads(Map<JobVertexID, GroupVertexCpuLoadSummary> loadSummaries) throws JSONException, IOException;

	protected long getLogTimestamp() {
		return QosUtils.alignToInterval(System.currentTimeMillis(), this.loggingInterval);
	}

	public void close() throws IOException {
	};
}
