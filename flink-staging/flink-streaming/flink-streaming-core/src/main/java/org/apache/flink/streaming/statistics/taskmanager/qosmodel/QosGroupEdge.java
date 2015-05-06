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


import org.apache.flink.runtime.jobgraph.DistributionPattern;

/**
 * This class models a Qos group edge as part of a Qos graph. It is equivalent
 * to an {@link org.apache.flink.runtime.jobgraph.JobEdge}.
 *
 * @author Bjoern Lohrmann
 */
public class QosGroupEdge {

	private DistributionPattern distributionPattern;

	private int outputGateIndex;

	private int inputGateIndex;

	private QosGroupVertex sourceVertex;

	private QosGroupVertex targetVertex;

	public QosGroupEdge(DistributionPattern distributionPattern,
			QosGroupVertex sourceVertex, QosGroupVertex targetVertex,
			int outputGateIndex, int inputGateIndex) {

		this.distributionPattern = distributionPattern;
		this.sourceVertex = sourceVertex;
		this.targetVertex = targetVertex;
		this.outputGateIndex = outputGateIndex;
		this.inputGateIndex = inputGateIndex;
	}

	public DistributionPattern getDistributionPattern() {
		return this.distributionPattern;
	}

	public QosGroupVertex getSourceVertex() {
		return this.sourceVertex;
	}

	public QosGroupVertex getTargetVertex() {
		return this.targetVertex;
	}

	public int getOutputGateIndex() {
		return this.outputGateIndex;
	}

	public int getInputGateIndex() {
		return this.inputGateIndex;
	}
}
