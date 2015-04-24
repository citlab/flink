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

import java.util.ArrayList;

import org.apache.flink.streaming.statistics.util.SparseDelegateIterable;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;

/**
 * This class models an input or output gate of a Qos vertex as part of a Qos
 * graph. It is equivalent to an
 * {@link org.apache.flink.runtime.executiongraph.ExecutionGate}.
 *
 * @author Bjoern Lohrmann
 *
 */
public class QosGate {

	private IntermediateDataSetID intermediateDataSetID;

	private int gateIndex;

	/**
	 * Sparse list of edges, which means this list may contain null entries.
	 */
	private ArrayList<QosEdge> edges;

	private QosVertex vertex;

	public enum GateType {
		INPUT_GATE, OUTPUT_GATE;
	}

	private GateType gateType;

	private int noOfEdges;

	/**
	 * Initializes QosGate.
	 *
	 */
	public QosGate(IntermediateDataSetID intermediateDataSetID, int gateIndex) {
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

	public void setGateType(GateType gateType) {
		this.gateType = gateType;
	}

	public GateType getGateType() {
		return this.gateType;
	}

	public boolean isInputGate() {
		return this.gateType == GateType.INPUT_GATE;
	}

	public boolean isOutputGate() {
		return this.gateType == GateType.OUTPUT_GATE;
	}

	public void addEdge(QosEdge edge) {
		int edgeIndex = this.isOutputGate() ? edge.getOutputGateEdgeIndex()
				: edge.getInputGateEdgeIndex();

		if (edgeIndex >= this.edges.size()) {
			this.fillWithNulls(this.edges, edgeIndex + 1);
		}

		if (this.edges.get(edgeIndex) == null) {
			this.noOfEdges++;
		}
		this.edges.set(edgeIndex, edge);
	}

	private <T> void fillWithNulls(ArrayList<T> list, int targetSize) {
		int toAdd = targetSize - list.size();

		for (int i = 0; i < toAdd; i++) {
			list.add(null);
		}
	}

	public SparseDelegateIterable<QosEdge> getEdges() {
		return new SparseDelegateIterable<QosEdge>(this.edges.iterator());
	}

	public QosEdge getEdge(int edgeIndex) {
		QosEdge toReturn = null;

		if (this.edges.size() > edgeIndex) {
			toReturn = this.edges.get(edgeIndex);
		}

		return toReturn;
	}

	public int getNumberOfEdges() {
		return this.noOfEdges;
	}

	public QosVertex getVertex() {
		return this.vertex;
	}

	public QosGate cloneWithoutEdgesAndVertex() {
		return new QosGate(this.intermediateDataSetID, this.gateIndex);
	}
}
