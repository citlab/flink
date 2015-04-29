/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.streaming.statistics;

import java.util.LinkedList;
import java.util.List;

import org.apache.flink.runtime.jobgraph.AbstractJobVertex;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;

public class JobGraphSequenceFinder {
	private JobGraph jobGraph;

	public JobGraphSequenceFinder(JobGraph jobGraph) {
		this.jobGraph = jobGraph;
	}

	/**
	 * Return a list of all possible {@link JobGraphSequence}s between two job vertices.
	 *
	 * @param beginId
	 * 		the id of the begin vertex.
	 * @param endId
	 * 		the id of the end vertex.
	 * @return list of job graph sequences.
	 */
	public List<JobGraphSequence> findAllSequencesBetween(JobVertexID beginId, JobVertexID endId) {
		AbstractJobVertex beginVertex = jobGraph.findVertexByID(beginId);
		AbstractJobVertex endVertex = jobGraph.findVertexByID(endId);

		return findAllSequencesBetween(beginVertex, endVertex);
	}

	/**
	 * Return a list of all possible {@link JobGraphSequence}s between two job vertices.
	 *
	 * @param beginVertex
	 * 		the begin vertex.
	 * @param endVertex
	 * 		the end vertex.
	 * @return list of job graph sequences.
	 */
	public List<JobGraphSequence> findAllSequencesBetween(AbstractJobVertex beginVertex, AbstractJobVertex endVertex) {

		IntermediateDataSet beginOutput = beginVertex.getProducedDataSets().get(0);
		AbstractJobVertex beginTargetVertex = beginOutput.getConsumers().get(0).getTarget();
		int beginTargetInputGate = getInputGateIndex(beginOutput, beginTargetVertex);

		IntermediateDataSet endOutput = endVertex.getProducedDataSets().get(0);
		AbstractJobVertex endTargetVertex = endOutput.getConsumers().get(0).getTarget();
		int endInputGate = getInputGateIndex(endOutput, endTargetVertex);

		JobGraphSequence stack = new JobGraphSequence();
		LinkedList<JobGraphSequence> result = new LinkedList<JobGraphSequence>();

		depthFirstSequenceEnumerate(
				beginVertex, 0,
				beginTargetVertex, beginTargetInputGate,
				stack, result, endTargetVertex, endInputGate);

		return result;
	}

	private void depthFirstSequenceEnumerate(
			AbstractJobVertex sourceVertex, int sourceOutputGate,
			AbstractJobVertex targetVertex, int targetInputGate,
			JobGraphSequence stack, LinkedList<JobGraphSequence> result, AbstractJobVertex endVertex, int endInputGate) {

		stack.addEdge(sourceVertex.getID(), sourceOutputGate, targetVertex.getID(), targetInputGate);

		if (targetVertex.getID().equals(endVertex.getID())) {
			// recursion ends here
			if (targetInputGate == endInputGate) {
				// only add stack to result if the last edge connects to the given input gate index of the end vertex
				result.add((JobGraphSequence) stack.clone());
			}
		} else {
			List<IntermediateDataSet> dataSets = targetVertex.getProducedDataSets();

			for (int i = 0; i < dataSets.size(); i++) {
				// uses assumptions about how a stream graph is converted into a job graph
				AbstractJobVertex newTargetVertex = dataSets.get(i).getConsumers().get(0).getTarget();
				int newTargetInputGate = getInputGateIndex(dataSets.get(i), newTargetVertex);

				stack.addVertex(targetVertex.getID(), targetVertex.getName(), targetInputGate, i);

				depthFirstSequenceEnumerate(
						targetVertex, i,
						newTargetVertex, newTargetInputGate,
						stack, result, endVertex, endInputGate);

				stack.removeLast();
			}
		}

		stack.removeLast();
	}

	private int getInputGateIndex(IntermediateDataSet source, AbstractJobVertex target) {
		List<JobEdge> inputs = target.getInputs();
		for (int i = 0; i < inputs.size(); i++) {
			JobEdge edge = inputs.get(i);
			if (edge.getSource().getId().equals(source.getId())) {
				return i;
			}
		}

		throw new RuntimeException(String.format("IntermediateDataSet (%s) and AbstractJobVertex (%s) are not connected",
				source.getId().toString(), target.getID().toString()
		));
	}
}