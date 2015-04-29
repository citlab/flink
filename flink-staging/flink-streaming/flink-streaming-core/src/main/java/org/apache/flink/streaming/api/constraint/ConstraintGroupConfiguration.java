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

package org.apache.flink.streaming.api.constraint;

import org.apache.flink.streaming.api.constraint.identifier.ConstraintGroupIdentifier;

/**
 * Configuration for a constraint group definition.
 */
public class ConstraintGroupConfiguration {
	/**
	 * Identifier of a constraint group.
	 */
	private ConstraintGroupIdentifier identifier;

	/**
	 * A stream graph edge marking the begin of a constraint group.
	 */
	private ConstraintBoundary start;

	/**
	 * A stream graph edge marking the end of a constraint group.
	 */
	private ConstraintBoundary end;

	/**
	 * The maximum latency for all constraint in the defined constraint group.
	 */
	private long maxLatency;

	/**
	 * Whether chaining over constraint boundaries should be disabled.
	 */
	private boolean chainingDisabled;

	public ConstraintGroupConfiguration(ConstraintGroupIdentifier identifier) {
		this.identifier = identifier;
		start = new ConstraintBoundary();
		end = new ConstraintBoundary();
	}

	public ConstraintGroupIdentifier getIdentifier() {
		return identifier;
	}

	public void setIdentifier(ConstraintGroupIdentifier identifier) {
		this.identifier = identifier;
	}

	public ConstraintBoundary getStart() {
		return start;
	}

	public void setStart(ConstraintBoundary start) {
		this.start = start;
	}

	public ConstraintBoundary getEnd() {
		return end;
	}

	public void setEnd(ConstraintBoundary end) {
		this.end = end;
	}

	public long getMaxLatency() {
		return maxLatency;
	}

	public void setMaxLatency(long maxLatency) {
		this.maxLatency = maxLatency;
	}

	public boolean isChainingDisabled() {
		return chainingDisabled;
	}

	public void setChainingDisabled(boolean chainingDisabled) {
		this.chainingDisabled = chainingDisabled;
	}
}
