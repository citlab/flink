/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.statistics;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.AbstractJobVertex;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.util.DataInputDeserializer;
import org.apache.flink.runtime.util.DataOutputSerializer;
import org.apache.flink.runtime.util.SerializableArrayList;

import java.io.IOException;
import java.util.LinkedList;

/**
 * This class contains utility methods to simplify the construction of
 * constraints.
 *
 * @author Bjoern Lohrmann
 */
public class ConstraintUtil {

	private final static String STREAMING_LATENCY_CONSTRAINTS_KEY = "flink.streaming.latency.constraints";

	/**
	 * Embeds a new latency constraint with the given sequence and maximum
	 * latency into the job configuration of the job graph.
	 *
	 * @param sequence
	 * @param maxLatencyInMillis
	 * @param jobGraph
	 * @throws IOException
	 *             If something goes wrong while serializing the constraints
	 *             (shouldn't happen).
	 * @throws IllegalArgumentException
	 *             If constraints could not be constructed to to invalid
	 *             input parameters.
	 */
	public static void defineLatencyConstraint(JobGraphSequence sequence,
			long maxLatencyInMillis, JobGraph jobGraph, String name) throws IOException {

		ensurePreconditions(sequence, jobGraph);

		JobGraphLatencyConstraint constraint = new JobGraphLatencyConstraint(
				sequence, maxLatencyInMillis, name);

		addConstraint(constraint, jobGraph);
	}

	public static void addConstraint(JobGraphLatencyConstraint constraint,
			JobGraph jobGraph) throws IOException {

		Configuration jobConfig = jobGraph.getJobConfiguration();
		SerializableArrayList<JobGraphLatencyConstraint> constraints = getConstraints(jobConfig);
		constraints.add(constraint);
		putConstraints(jobConfig, constraints);
	}

	private static void ensurePreconditions(JobGraphSequence sequence,
			JobGraph jobGraph) {

		if (sequence.size() < 3) {
			throw new IllegalArgumentException(
					"Cannot define latency constraint on short sequence."
					+" At least 3 elements required.");
		}

		if (sequence.getFirst().isVertex()) {
			for (AbstractJobVertex inputVertex : getInputVertices(jobGraph)) {
				if (sequence.getFirst().getVertexID().equals(inputVertex.getID())) {
					throw new IllegalArgumentException(
							"Cannot define latency constraint that includes an input vertex. ");
				}
			}
		}

		if (sequence.getLast().isVertex()) {
			for (AbstractJobVertex outputVertex : getOutputVertices(jobGraph)) {
				if (sequence.getLast().getVertexID().equals(outputVertex.getID())) {
					throw new IllegalArgumentException(
							"Cannot define latency constraint that includes an output vertex. ");
				}
			}
		}
	}

	private static void putConstraints(Configuration jobConfiguration,
			SerializableArrayList<JobGraphLatencyConstraint> constraints)
			throws IOException {

		DataOutputSerializer bytesOut = new DataOutputSerializer(10*1024);
		constraints.write(bytesOut);
		jobConfiguration.setBytes(STREAMING_LATENCY_CONSTRAINTS_KEY,
				bytesOut.wrapAsByteBuffer().array());
	}

	public static SerializableArrayList<JobGraphLatencyConstraint> getConstraints(
			Configuration jobConfiguration) throws IOException {

		byte[] bytes = jobConfiguration.getBytes(STREAMING_LATENCY_CONSTRAINTS_KEY, null);
		SerializableArrayList<JobGraphLatencyConstraint> list = new SerializableArrayList<JobGraphLatencyConstraint>();

		if (bytes != null) {
			DataInputDeserializer bytesIn = new DataInputDeserializer(bytes, 0, bytes.length);
			list.read(bytesIn);
		}

		return list;
	}

	/**
	 * Returns an Iterable to iterate all input vertices registered with the job graph.
	 */
	public static Iterable<AbstractJobVertex> getInputVertices(JobGraph jobGraph) {
		final LinkedList<AbstractJobVertex> inputVertices = new LinkedList<AbstractJobVertex>();

		for(AbstractJobVertex vertex : jobGraph.getVertices()) {
			if (vertex.isInputVertex()) {
				inputVertices.add(vertex);
			}
		}

		return inputVertices;
	}

	/**
	 * Returns an Iterable to iterate all output vertices registered with the job graph.
	 */
	public static Iterable<AbstractJobVertex> getOutputVertices(JobGraph jobGraph) {
		final LinkedList<AbstractJobVertex> outputVertices = new LinkedList<AbstractJobVertex>();

		for(AbstractJobVertex vertex : jobGraph.getVertices()) {
			if (vertex.isOutputVertex()) {
				outputVertices.add(vertex);
			}
		}

		return outputVertices;
	}
}
