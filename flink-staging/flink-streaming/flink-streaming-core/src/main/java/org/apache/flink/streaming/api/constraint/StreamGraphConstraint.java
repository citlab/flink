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

package org.apache.flink.streaming.api.constraint;

import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.constraint.identifier.ConstraintIdentifier;
import org.apache.flink.streaming.api.constraint.identifier.NamedConstraintIdentifier;

/**
 * Represents a constraint on a {@link StreamGraph}. It consists of the {@link StreamGraphSequence}s which are affected
 * by the constraint and a maximum latency.
 */
public class StreamGraphConstraint {

	private final ConstraintIdentifier id;

	private final StreamGraphSequence sequence;

	private final long latencyConstraintInMillis;

	private final int seqIndex; // -1 if only one sequence (path) on graph within constraint

	public StreamGraphConstraint(ConstraintIdentifier id, StreamGraphSequence sequence, long latencyConstraintInMillis, int seqIndex) {
		this.id = id;
		this.sequence = sequence;
		this.latencyConstraintInMillis = latencyConstraintInMillis;
		this.seqIndex = seqIndex;
	}

	public StreamGraphConstraint(ConstraintIdentifier id, StreamGraphSequence sequence, long latencyConstraintInMillis) {
		this(id, sequence, latencyConstraintInMillis, -1);
	}

	public StreamGraphSequence getSequence() {
		return sequence;
	}

	public long getLatencyConstraintInMillis() {
		return latencyConstraintInMillis;
	}

	public String getName(String firstVertex, String lastVertex) {
		if (this.seqIndex >= 0) {
			return String.format("%s (%d)", getNamePrefix(firstVertex, lastVertex), this.seqIndex);
		} else {
			return getNamePrefix(firstVertex, lastVertex);
		}
	}

	private String getNamePrefix(String firstVertex, String lastVertex) {
		if (this.id instanceof NamedConstraintIdentifier) {
			return ((NamedConstraintIdentifier) this.id).getName();
		} else {
			return String.format("%s -> %s", firstVertex, lastVertex);
		}
	}

	@Override
	public int hashCode() {
		return this.id.hashCode() ^ this.seqIndex;
	}

	@Override
	public boolean equals(Object other) {
		return (other instanceof StreamGraphConstraint)
				&& ((StreamGraphConstraint)other).id.equals(this.id)
				&& ((StreamGraphConstraint)other).seqIndex == this.seqIndex;
	}
}
