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

import org.apache.flink.runtime.jobgraph.AbstractJobVertex;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.streaming.api.graph.StreamConfig;

import java.util.LinkedList;
import java.util.List;

public class JobGraphSequenceFinder {

	/**
	 * Return a list of all possible {@link JobGraphSequence}s between two job graph edges.
	 *
	 * @param beginSourceVertex
	 * 		the source vertex of the begin edge.
	 * @param beginTargetVertex
	 * 		the target vertex of the begin edge.
	 * @param includeBeginVertex
	 * 		whether the beginSourceVertex should be included in the result sequences.
	 * @param endSourceVertex
	 * 		the source vertex of the end edge.
	 * @param endTargetVertex
	 * 		the target vertex of the end edge
	 * @param includeEndVertex
	 * 		whether the endTargetVertex should be included in the result sequences.
	 * @return list of job graph sequences.
	 */
	public List<JobGraphSequence> findAllSequencesBetween(
			AbstractJobVertex beginSourceVertex, AbstractJobVertex beginTargetVertex, boolean includeBeginVertex,
			AbstractJobVertex endSourceVertex, AbstractJobVertex endTargetVertex, boolean includeEndVertex) {

		int beginSourceOutputGate = getOutputGateIndex(beginSourceVertex, beginTargetVertex);
		IntermediateDataSet beginOutput = beginSourceVertex.getProducedDataSets().get(beginSourceOutputGate);
		int beginTargetInputGate = getInputGateIndex(beginOutput, beginTargetVertex);

		int endSourceOutputGate = getOutputGateIndex(endSourceVertex, endTargetVertex);
		IntermediateDataSet endOutput = endSourceVertex.getProducedDataSets().get(endSourceOutputGate);
		int endTargetInputGate = getInputGateIndex(endOutput, endTargetVertex);

		JobGraphSequence stack = new JobGraphSequence();
		if (includeBeginVertex) {
			stack.addVertex(beginSourceVertex.getID(), beginSourceVertex.getName(), -1, beginSourceOutputGate);
		}
		LinkedList<JobGraphSequence> result = new LinkedList<JobGraphSequence>();

		depthFirstSequenceEnumerate(
				beginSourceVertex, beginSourceOutputGate,
				beginTargetVertex, beginTargetInputGate,
				stack, result, endTargetVertex, endTargetInputGate);

		if (includeEndVertex) {
			for (JobGraphSequence sequence : result) {
				sequence.addVertex(endTargetVertex.getID(), endTargetVertex.getName(), endTargetInputGate, -1);
			}
		}

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

				StreamConfig streamConfig = new StreamConfig(targetVertex.getConfiguration());
				stack.addVertex(targetVertex.getID(), targetVertex.getName(), targetInputGate, i,
						streamConfig.getSamplingStrategy());

				depthFirstSequenceEnumerate(
						targetVertex, i,
						newTargetVertex, newTargetInputGate,
						stack, result, endVertex, endInputGate);

				stack.removeLast();
			}
		}

		stack.removeLast();
	}

	private int getOutputGateIndex(AbstractJobVertex source, AbstractJobVertex target) {
		List<IntermediateDataSet> producedDataSets = source.getProducedDataSets();
		for (int i = 0; i < producedDataSets.size(); i++) {
			for (JobEdge edge : producedDataSets.get(i).getConsumers()) {
				if (edge.getTarget().equals(target)) {
					return i;
				}
			}
		}

		throw new RuntimeException(String.format("AbstractJobVertex (%s) and AbstractJobVertex (%s) are not connected",
				source.getID().toString(), target.getID().toString()
		));
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