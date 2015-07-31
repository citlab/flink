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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.streaming.statistics.ConstraintUtil;
import org.apache.flink.streaming.statistics.JobGraphLatencyConstraint;
import org.apache.flink.streaming.statistics.LatencyConstraintID;
import org.apache.flink.streaming.statistics.SequenceElement;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Bjoern Lohrmann, Sascha Wolke
 */
public class QosGraph {

	private final QosGraphID qosGraphID;

	private final HashMap<LatencyConstraintID, JobGraphLatencyConstraint> constraints;

	private final HashMap<JobVertexID, QosGroupVertex> vertexByID;

	public QosGraph() {
		this.qosGraphID = new QosGraphID();
		this.constraints = new HashMap<LatencyConstraintID, JobGraphLatencyConstraint>();
		this.vertexByID = new HashMap<JobVertexID, QosGroupVertex>();
	}

	public void addConstraint(JobGraphLatencyConstraint constraint) {
		this.ensureGraphContainsConstrainedVertices(constraint);
		this.constraints.put(constraint.getID(), constraint);
	}

	private void ensureGraphContainsConstrainedVertices(JobGraphLatencyConstraint constraint) {
		for (SequenceElement seqElem : constraint.getSequence()) {
			if (seqElem.isEdge()) {
				if (!this.vertexByID.containsKey(seqElem.getSourceVertexID())
						|| !this.vertexByID.containsKey(seqElem.getTargetVertexID())) {

					throw new IllegalArgumentException(
							"Cannot add constraint to a graph that does not contain the constraint's vertices. This is a bug.");
				}
			}
		}
	}

	public Collection<JobGraphLatencyConstraint> getConstraints() {
		return this.constraints.values();
	}

	public Map<LatencyConstraintID, JobGraphLatencyConstraint> getConstraintsWithId() {
		return this.constraints;
	}

	public JobGraphLatencyConstraint getConstraintByID(
			LatencyConstraintID constraintID) {
		return this.constraints.get(constraintID);
	}

	public QosGroupVertex getOrCreateGroupVertex(ExecutionJobVertex execVertex) {
		QosGroupVertex groupVertex = this.vertexByID.get(execVertex.getJobVertexId());

		if (groupVertex == null) {
			groupVertex = new QosGroupVertex(
					execVertex.getJobVertexId(), execVertex.getJobVertex().getName());
			this.vertexByID.put(execVertex.getJobVertexId(), groupVertex);
		}

		return groupVertex;
	}

	public QosGroupVertex getGroupVertexByID(JobVertexID vertexID) {
		return this.vertexByID.get(vertexID);
	}

	public Collection<QosGroupVertex> getAllVertices() {
		return Collections.unmodifiableCollection(this.vertexByID.values());
	}

	public int getNumberOfVertices() {
		return this.vertexByID.size();
	}

	public QosGraphID getQosGraphID() {
		return this.qosGraphID;
	}

	@Override
	public int hashCode() {
		return this.qosGraphID.hashCode();
	}

	@Override
	public boolean equals(Object other) {
		return other != null
			&& other instanceof QosGraph
			&& this.qosGraphID.equals(((QosGraph) other).qosGraphID);
	}

	/**
	 * Determines whether this Qos graph is shallow. A graph is shallow, if at
	 * least one of the group vertices does not have any members.
	 *
	 * @return whether this Qos graph is shallow or not.
	 */
	public boolean isShallow() {
		for (QosGroupVertex groupVertex : this.getAllVertices()) {
			if (groupVertex.getNumberOfMembers() == 0) {
				return true;
			}
		}

		return false;
	}

	/**
	 * Adds the smallest possible subgraph of the given execution graph, where
	 * all vertices and edges are affected by the given constraint. If the
	 * constraint starts/ends with and edge the respective source/target vertex
	 * is also part of the QosGraph, although it is not strictly part of the
	 * constraint.
	 *
	 * @param execGraph
	 *            An execution graph.
	 * @param constraint
	 *            A latency constraint the affects elements of the given
	 *            execution graph.
	 */
	private void addConstrainedQosGraph(
			ExecutionGraph execGraph, JobGraphLatencyConstraint constraint) {

		ExecutionJobVertex currExecVertex = null;
		QosGroupVertex currGroupVertex = null;

		for (SequenceElement sequenceElem : constraint.getSequence()) {

			if (currExecVertex == null) {
				JobVertexID firstJobVertexID;

				if (sequenceElem.isVertex()) {
					firstJobVertexID = sequenceElem.getVertexID();
				} else {
					firstJobVertexID = sequenceElem.getSourceVertexID();
				}

				currExecVertex = execGraph.getJobVertex(firstJobVertexID);
				currGroupVertex = getOrCreateGroupVertex(currExecVertex);
			}

			if (sequenceElem.isEdge()) {
				int outputGate = sequenceElem.getOutputGateIndex();
				int inputGate = sequenceElem.getInputGateIndex();

				ExecutionJobVertex nextExecVertex = execGraph.getJobVertex(sequenceElem.getTargetVertexID());
				QosGroupVertex nextGroupVertex = getOrCreateGroupVertex(nextExecVertex);

				if (currGroupVertex.getForwardEdge(outputGate) == null) {
					QosGroupEdge qosEdge = new QosGroupEdge(
							extractDistributionPattern(currExecVertex, nextExecVertex, outputGate, inputGate),
							currGroupVertex, nextGroupVertex, outputGate, inputGate
					);
					currGroupVertex.setForwardEdge(qosEdge);
					nextGroupVertex.setBackwardEdge(qosEdge);
				}

				currExecVertex = nextExecVertex;
				currGroupVertex = nextGroupVertex;
			}
		}

		addConstraint(constraint);
	}

	// TODO: validate seq -> targetIndex
	private DistributionPattern extractDistributionPattern(
			ExecutionJobVertex sourceVertex, ExecutionJobVertex targetVertex,
			int outputGateIndex, int inputGateIndex) {

		List<JobEdge> edges = sourceVertex.getJobVertex()
				.getProducedDataSets().get(outputGateIndex).getConsumers();

		for (JobEdge edge : edges) {
			if (targetVertex.getJobVertexId().equals(edge.getTarget().getID())) {
				return edge.getDistributionPattern();
			}
		}

		throw new RuntimeException("Can't identify distribution pattern. This is a Bug.");
	}

	/**
	 * Builds a qos graph with group vertices and edges based on constraints from job configuration.
	 * Only job and not execution specific parts are added.
	 *
	 * @return job graph contains group vertices, edges and constraints
	 */
	public static QosGraph buildQosGraphFromJobConfig(ExecutionGraph execGraph) {
		QosGraph qosGraph = new QosGraph();

		try {
			Configuration jobConfig = execGraph.getJobConfiguration();

			for (JobGraphLatencyConstraint constraint : ConstraintUtil.getConstraints(jobConfig)) {
				qosGraph.addConstrainedQosGraph(execGraph, constraint);
			}

		} catch(Exception e) {
			throw new RuntimeException("Can't read qos constrain from job config.", e);
		}

		return qosGraph;
	}
}
