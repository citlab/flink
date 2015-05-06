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

package org.apache.flink.streaming.statistics.taskmanager.qosmodel;

import org.apache.flink.streaming.statistics.message.action.QosReporterConfig;
import org.apache.flink.streaming.statistics.message.qosreport.AbstractQosReportRecord;
import org.apache.flink.streaming.statistics.message.qosreport.EdgeLatency;
import org.apache.flink.streaming.statistics.message.qosreport.EdgeStatistics;

/**
 * This class models a Qos edge as part of a Qos graph. It is equivalent to an
 * {@link org.apache.flink.runtime.executiongraph.ExecutionEdge}.
 *
 * One {@link QosReporterID.Edge} represents this edge.
 * Statistics can be send from two different task managers.
 *
 * This data class residents on job manager side and is never transferred.
 *
 * @author Bjoern Lohrmann, Sascha Wolke
 *
 */
public class QosEdge implements QosGraphMember {

	private final QosReporterID.Edge reporterID;

	private QosGate outputGate;

	private QosGate inputGate;

	private EdgeQosData qosData;

	public QosEdge(QosReporterID.Edge reporterID) {
		this.reporterID = reporterID;
	}

	public QosReporterID.Edge getReporterID() {
		return reporterID;
	}

	/**
	 * Returns the outputGate.
	 *
	 * @return the outputGate
	 */
	public QosGate getOutputGate() {
		return this.outputGate;
	}

	/**
	 * Sets the outputGate to the specified value.
	 *
	 * @param outputGate
	 *            the outputGate to set
	 */
	public void setOutputGate(QosGate outputGate) {
		this.outputGate = outputGate;
		this.outputGate.addEdge(this);
	}

	/**
	 * Returns the inputGate.
	 *
	 * @return the inputGate
	 */
	public QosGate getInputGate() {
		return this.inputGate;
	}

	/**
	 * Sets the inputGate to the specified value.
	 *
	 * @param inputGate
	 *            the inputGate to set
	 */
	public void setInputGate(QosGate inputGate) {
		this.inputGate = inputGate;
		this.inputGate.addEdge(this);
	}

	/**
	 * Returns the outputGateEdgeIndex.
	 *
	 * @return the outputGateEdgeIndex
	 */
	public int getOutputGateEdgeIndex() {
		if (this.outputGate != null) {
			return this.outputGate.getGateIndex();
		} else {
			return -1;
		}
	}

	/**
	 * Returns the inputGateEdgeIndex.
	 *
	 * @return the inputGateEdgeIndex
	 */
	public int getInputGateEdgeIndex() {
		if (this.inputGate != null) {
			return this.inputGate.getGateIndex();
		} else {
			return -1;
		}
	}

	@Override
	public EdgeQosData getQosData() {
		return this.qosData;
	}

	public void setQosData(EdgeQosData qosData) {
		this.qosData = qosData;
	}

	@Override
	public void processStatistics(QosReporterConfig reporterConfig,
			AbstractQosReportRecord statistic, long now) {

		if (statistic instanceof EdgeStatistics) {
			this.qosData.addOutputChannelStatisticsMeasurement(now, (EdgeStatistics) statistic);

		} else if (statistic instanceof EdgeLatency) {
			this.qosData.addLatencyMeasurement(now, ((EdgeLatency)statistic).getEdgeLatency());

		} else {
			throw new RuntimeException("Unknown statistic type received: "
					+ statistic.getClass().getSimpleName());
		}
	}

	@Override
	public String toString() {
		return String.format("%s->%s", this.getOutputGate().getVertex()
				.getName(), this.getInputGate().getVertex().getName());
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (this.getClass() != obj.getClass()) {
			return false;
		}

		QosEdge other = (QosEdge) obj;
		return this.reporterID.equals(other.reporterID);
	}

	@Override
	public boolean isVertex() {
		return false;
	}

	@Override
	public boolean isEdge() {
		return true;
	}
}
