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

package org.apache.flink.streaming.statistics.taskmanager.qosmanager;

import java.util.List;

public class QosGroupVertexSummary implements QosGroupElementSummary {

	private int activeVertices = 0;

	private double meanVertexLatency = 0;

	private double meanVertexLatencyCV = 0;

	public double getMeanVertexLatency() {
		return meanVertexLatency;
	}

	public void setMeanVertexLatency(double meanVertexLatency) {
		this.meanVertexLatency = meanVertexLatency;
	}

	public double getMeanVertexLatencyCV() {
		return meanVertexLatencyCV;
	}

	public void setMeanVertexLatencyCV(double meanVertexLatencyCV) {
		this.meanVertexLatencyCV = meanVertexLatencyCV;
	}

	public int getActiveVertices() {
		return activeVertices;
	}

	public void setActiveVertices(int activeVertices) {
		this.activeVertices = activeVertices;
	}

	@Override
	public boolean isVertex() {
		return true;
	}

	@Override
	public boolean isEdge() {
		return false;
	}

	@Override
	public void merge(List<QosGroupElementSummary> elemSummaries) {
		for (QosGroupElementSummary elemSum : elemSummaries) {
			QosGroupVertexSummary toMerge = (QosGroupVertexSummary) elemSum;
			
			activeVertices += toMerge.activeVertices;
			
			meanVertexLatency += toMerge.activeVertices
					* toMerge.meanVertexLatency;
			
			meanVertexLatencyCV += toMerge.activeVertices
					* toMerge.meanVertexLatencyCV;
		}

		if (activeVertices > 0) {
			meanVertexLatency /= activeVertices;
			meanVertexLatencyCV /= activeVertices;
		}
	}

	@Override
	public boolean hasData() {
		return activeVertices > 0;
	}
}
