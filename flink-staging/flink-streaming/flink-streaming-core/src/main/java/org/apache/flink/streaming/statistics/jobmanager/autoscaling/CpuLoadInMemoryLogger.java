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
import org.apache.flink.streaming.statistics.taskmanager.qosmanager.buffers.HistoryEntry;
import org.apache.flink.streaming.statistics.taskmanager.qosmanager.buffers.ValueHistory;
import org.apache.flink.streaming.statistics.util.QosStatisticsConfig;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.Map;

public class CpuLoadInMemoryLogger extends AbstractCpuLoadLogger {
	private final ValueHistory<JSONObject> history;
	private final JSONArray header;

	public CpuLoadInMemoryLogger(ExecutionGraph execGraph, JobGraphLatencyConstraint constraint, long loggingInterval) throws JSONException {
		super(execGraph, constraint, loggingInterval);

		this.header = getHeader(execGraph);

		this.history = new ValueHistory<JSONObject>(QosStatisticsConfig.getNoOfInMemoryLogEntries());
	}

	private JSONArray getHeader(ExecutionGraph execGraph) throws JSONException {
		JSONArray header = new JSONArray();

		for (JobVertexID id : this.groupVertices) {
			JSONObject vertex = new JSONObject();
			vertex.put("id", id);
			vertex.put("name", execGraph.getJobVertex(id).getJobVertex().getName());
		 	header.put(vertex);
		}

		return header;
	}

	@Override
	public void logCpuLoads(Map<JobVertexID, GroupVertexCpuLoadSummary> loadSummaries) throws JSONException {
		long timestamp = getLogTimestamp();
		JSONObject entry = new JSONObject();
		entry.put("ts", timestamp);
		JSONArray values = new JSONArray();

		for (JobVertexID id : this.groupVertices) {
			JSONArray vertexValues = new JSONArray();
			GroupVertexCpuLoadSummary cpuLoad = loadSummaries.get(id);
			vertexValues.put(cpuLoad.getUnknowns());
			vertexValues.put(cpuLoad.getLows());
			vertexValues.put(cpuLoad.getMediums());
			vertexValues.put(cpuLoad.getHighs());
			values.put(vertexValues);
		}

		entry.put("values", values);

		this.history.addToHistory(timestamp, entry);
	}

	public JSONObject toJson(JSONObject json) throws JSONException {
		return toJson(json, this.history.getEntries(), true);
	}

	public JSONObject toJson(JSONObject json, long minTimestamp) throws JSONException {
		return toJson(json, this.history.getLastEntries(minTimestamp), false);
	}

	private JSONObject toJson(JSONObject result, HistoryEntry<JSONObject> entries[]
			, boolean withLabels) throws JSONException {

		JSONArray values = new JSONArray();

		for (HistoryEntry<JSONObject> entry : entries) {
			values.put(entry.getValue());
		}

		JSONObject cpuLoads = new JSONObject();
		if (withLabels) {
			cpuLoads.put("header", this.header);
		}
		cpuLoads.put("values", values);
		result.put("cpuLoads", cpuLoads);

		return result;
	}
}
