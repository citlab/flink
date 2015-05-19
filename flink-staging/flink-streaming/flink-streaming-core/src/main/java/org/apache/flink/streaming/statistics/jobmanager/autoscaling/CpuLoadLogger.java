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
import org.apache.flink.streaming.statistics.util.QosStatisticsConfig;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

public class CpuLoadLogger extends AbstractCpuLoadLogger {
	private BufferedWriter writer;

	public CpuLoadLogger(ExecutionGraph execGraph, JobGraphLatencyConstraint constraint, long loggingInterval) throws IOException {
		super(execGraph, constraint, loggingInterval);
		
		String logFile = QosStatisticsConfig.getCpuStatisticsLogfilePattern();
		if (logFile.contains("%s")) {
			JobID jobId = execGraph.getJobID();
			logFile = String.format(logFile, jobId, Integer.toString(constraint.getIndex()));
		}
		this.writer = new BufferedWriter(new FileWriter(logFile));
		this.writeHeaders(execGraph);
	}

	private String formatDouble(double doubleValue) {
		return String.format("%.2f", doubleValue);
	}

	@Override
	public void logCpuLoads(Map<JobVertexID, GroupVertexCpuLoadSummary> loadSummaries) throws IOException {
		StringBuilder sb = new StringBuilder();
		sb.append(getLogTimestamp() / 1000);
		for(JobVertexID id : this.groupVertices) {
			GroupVertexCpuLoadSummary summary = loadSummaries.get(id);
			sb.append(';');
			sb.append(formatDouble(summary.getAvgCpuUtilization()));
			sb.append(';');
			sb.append(summary.getHighs());
			sb.append(';');
			sb.append(summary.getMediums());
			sb.append(';');
			sb.append(summary.getLows());
			sb.append(';');
			sb.append(summary.getUnknowns());
		}
		
		sb.append('\n');
		
		this.writer.write(sb.toString());
		this.writer.flush();
	}
	
	private void writeHeaders(ExecutionGraph execGraph) throws IOException {
		StringBuilder sb = new StringBuilder();
		sb.append("timestamp");
		
		for(JobVertexID id : this.groupVertices) {
			String name = execGraph.getJobVertex(id).getJobVertex().getName();
			for(String type : new String[]{"avgUtil","high", "medium", "low", "unknown"}) {
				sb.append(';');
				sb.append(name);
				sb.append(':');
				sb.append(type);
			}
		}
		
		sb.append('\n');
		
		this.writer.write(sb.toString());
		this.writer.flush();
	}
	
	public void close() throws IOException {
		this.writer.close();
	}

}
