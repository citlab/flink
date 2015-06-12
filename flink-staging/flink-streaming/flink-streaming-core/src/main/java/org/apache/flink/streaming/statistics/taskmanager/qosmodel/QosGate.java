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

import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.streaming.statistics.util.SparseDelegateIterable;

import java.util.ArrayList;

/**
 * This class models an input or output gate of a Qos vertex as part of a Qos
 * graph. It is equivalent to an
 * {@link org.apache.flink.runtime.executiongraph.ExecutionGate}.
 *
 * @author Bjoern Lohrmann
 */
public class QosGate {

	private final IntermediateDataSetID intermediateDataSetID;

	private final int gateIndex;

	private final ArrayList<QosEdge> edges;

	private QosVertex vertex;

	public enum GateType {
		INPUT_GATE, OUTPUT_GATE;
	}

	private final GateType gateType;

	private int noOfEdges;

	public QosGate(GateType gateType, IntermediateDataSetID intermediateDataSetID, int gateIndex) {
		this.gateType = gateType;
		this.intermediateDataSetID = intermediateDataSetID;
		this.gateIndex = gateIndex;
		this.edges = new ArrayList<QosEdge>();
		this.noOfEdges = 0;
	}

	public IntermediateDataSetID getIntermediateDataSetID() {
		return intermediateDataSetID;
	}

	public int getGateIndex() {
		return this.gateIndex;
	}

	public void setVertex(QosVertex vertex) {
		this.vertex = vertex;
	}

	public boolean isInputGate() {
		return this.gateType == GateType.INPUT_GATE;
	}

	public boolean isOutputGate() {
		return this.gateType == GateType.OUTPUT_GATE;
	}

	public void addEdge(QosEdge edge) {
		this.edges.add(edge);
	}

	public SparseDelegateIterable<QosEdge> getEdges() {
		return new SparseDelegateIterable<QosEdge>(this.edges.iterator());
	}

	public QosVertex getVertex() {
		return this.vertex;
	}
}
