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

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.streaming.statistics.util.SparseDelegateIterable;

import java.util.ArrayList;

/**
 * This class models a Qos group vertex as part of a Qos graph. It is equivalent
 * to an {@link org.apache.flink.runtime.executiongraph.ExecutionJobVertex},
 * which in turn is equivalent to a
 * {@link org.apache.flink.runtime.jobgraph.AbstractJobVertex}.
 *
 * Some structures inside the group vertex are sparse. This applies to the lists
 * of forward and backward group edges as well as the member vertex list.
 * Sparseness means that, for example if the group vertex has an forward edge
 * with index 1 it may not have a forward edge with index 0. This stems from the
 * fact, that the Qos graph itself only contains those group vertices and group
 * edges from the executiong graph, that are covered by a constraint.
 *
 * @author Bjoern Lohrmann
 *
 */
public class QosGroupVertex {

	private final String name;

	/**
	 * The ID of the job vertex which is represented by this group vertex.
	 */
	private final JobVertexID jobVertexID;

	/**
	 * The list of {@link QosVertex} contained in this group vertex. This is a
	 * sparse list, meaning that entries in this list may be null.
	 */
	private ArrayList<QosVertex> groupMembers;

	/**
	 * The number of non-null members in this group vertex ({@see #groupMembers}
	 * )
	 */
	private int noOfMembers;

	/**
	 * The list of group edges which originate from this group vertex. This is a
	 * sparse list, meaning that entries in this list may be null.
	 */
	private ArrayList<QosGroupEdge> forwardEdges;

	/**
	 * The number of output gates that the member vertices have. This is equal
	 * to the number of non-null entries in forwardEdges.
	 */
	private int noOfOutputGates;

	/**
	 * The a group edge which arrives at this group vertex.
	 */
	private ArrayList<QosGroupEdge> backwardEdges;

	/**
	 * The number of input gates that the member vertices have. This is equal to
	 * the number of non-null entries in backwardEdges.
	 */
	private int noOfInputGates;

	public QosGroupVertex(JobVertexID jobVertexID, String name) {
		this.name = name;
		this.jobVertexID = jobVertexID;
		this.noOfMembers = 0;
		this.groupMembers = new ArrayList<QosVertex>();
		this.forwardEdges = new ArrayList<QosGroupEdge>();
		this.backwardEdges = new ArrayList<QosGroupEdge>();
	}

	public QosGroupEdge getForwardEdge(int outputGateIndex) {
		if (outputGateIndex < this.forwardEdges.size()) {
			return this.forwardEdges.get(outputGateIndex);
		} else {
			return null;
		}
	}

	public void setForwardEdge(QosGroupEdge forwardEdge) {
		int outputGate = forwardEdge.getOutputGateIndex();
		if (outputGate >= this.forwardEdges.size()) {
			this.fillWithNulls(this.forwardEdges, outputGate + 1);
		}

		if (this.forwardEdges.get(outputGate) == null) {
			this.noOfOutputGates++;
		}
		this.forwardEdges.set(outputGate, forwardEdge);
	}

	private <T> void fillWithNulls(ArrayList<T> list, int targetSize) {
		int toAdd = targetSize - list.size();

		for (int i = 0; i < toAdd; i++) {
			list.add(null);
		}
	}

	public QosGroupEdge getBackwardEdge(int inputGateIndex) {
		if (inputGateIndex < this.backwardEdges.size()) {
			return this.backwardEdges.get(inputGateIndex);
		} else {
			return null;
		}
	}

	public void setBackwardEdge(QosGroupEdge backwardEdge) {
		int inputGate = backwardEdge.getInputGateIndex();
		if (inputGate >= this.backwardEdges.size()) {
			this.fillWithNulls(this.backwardEdges, inputGate + 1);
		}

		if (this.backwardEdges.get(inputGate) == null) {
			this.noOfInputGates++;
		}

		this.backwardEdges.set(inputGate, backwardEdge);
	}

	public String getName() {
		return this.name;
	}

	public JobVertexID getJobVertexID() {
		return this.jobVertexID;
	}

	public Iterable<QosVertex> getMembers() {
		return new SparseDelegateIterable<QosVertex>(
				this.groupMembers.iterator());
	}

	public void setGroupMember(QosVertex groupMember) {
		this.setGroupMember(groupMember.getMemberIndex(), groupMember);
	}

	public void setGroupMember(int memberIndex, QosVertex groupMember) {
		if (this.groupMembers.size() <= memberIndex) {
			this.fillWithNulls(this.groupMembers, memberIndex + 1);
		}

		if (groupMember == null) {
			if (this.groupMembers.get(memberIndex) != null) {
				this.noOfMembers--;
			}
			this.groupMembers.set(memberIndex, null);
		} else {
			if (this.groupMembers.get(memberIndex) == null) {
				this.noOfMembers++;
			}
			groupMember.setGroupVertex(this);
			this.groupMembers.set(groupMember.getMemberIndex(), groupMember);
		}
	}

	@Override
	public String toString() {
		return this.name;
	}

	@Override
	public int hashCode() {
		return this.jobVertexID.hashCode();
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
		QosGroupVertex other = (QosGroupVertex) obj;
		return this.jobVertexID.equals(other.jobVertexID);
	}

	public int getNumberOfOutputGates() {
		return this.noOfOutputGates;
	}

	public Iterable<QosGroupEdge> getForwardEdges() {
		return new SparseDelegateIterable<QosGroupEdge>(
				this.forwardEdges.iterator());
	}

	public Iterable<QosGroupEdge> getBackwardEdges() {
		return new SparseDelegateIterable<QosGroupEdge>(
				this.backwardEdges.iterator());
	}

	public int getNumberOfInputGates() {
		return this.noOfInputGates;
	}

	public boolean hasOutputGate(int outputGateIndex) {
		return this.getForwardEdge(outputGateIndex) != null;
	}

	public boolean hasInputGate(int inputGateIndex) {
		return this.getBackwardEdge(inputGateIndex) != null;
	}

	/**
	 * @return The member with the given index, or null if there is no member at
	 *         the given index.
	 */
	public QosVertex getMember(int memberIndex) {
		if (memberIndex < this.groupMembers.size()) {
			return this.groupMembers.get(memberIndex);
		} else {
			return null;
		}
	}

	/**
	 * @return The number of actual (non-null) members in this group vertex.
	 */
	public int getNumberOfMembers() {
		return this.noOfMembers;
	}
}
