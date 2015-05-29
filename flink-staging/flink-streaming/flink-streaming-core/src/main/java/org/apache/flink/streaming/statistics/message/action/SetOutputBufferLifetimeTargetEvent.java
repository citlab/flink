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

package org.apache.flink.streaming.statistics.message.action;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import java.io.IOException;
import java.io.Serializable;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This class implements an action to communicate the desired output buffer
 * lifetime of a particular output channel to the channel itself.
 *
 * @author warneke, Bjoern Lohrmann, Sascha Wolke
 */
public final class SetOutputBufferLifetimeTargetEvent extends TaskEvent implements Serializable {

	private final ExecutionAttemptID attemptID;

	private final IntermediateResultPartitionID partitionID;

	private int outputBufferLifetimeTarget;

	public SetOutputBufferLifetimeTargetEvent(ExecutionAttemptID attemptID,
			IntermediateResultPartitionID partitionID, int outputBufferLifetimeTarget) {

		super();

		checkNotNull(attemptID, "Argument attemptID must not be null");
		checkNotNull(partitionID, "Argument partitionID must not be null");
		checkArgument(outputBufferLifetimeTarget >= 0, "Argument outputBufferLifetimeTarget must be greater or equal than zero");

		this.attemptID = attemptID;
		this.partitionID = partitionID;
		this.outputBufferLifetimeTarget = outputBufferLifetimeTarget;
	}

	public SetOutputBufferLifetimeTargetEvent() {
		super();
		this.attemptID = new ExecutionAttemptID();
		this.partitionID = new IntermediateResultPartitionID();
		this.outputBufferLifetimeTarget = 0;
	}

	public ExecutionAttemptID getAttemptID() {
		return attemptID;
	}

	public IntermediateResultPartitionID getPartitionID() {
		return partitionID;
	}

	public int getOutputBufferLifetimeTarget() {
		return outputBufferLifetimeTarget;
	}

	@Override
	public void read(DataInputView in) throws IOException {
		this.attemptID.read(in);
		this.partitionID.read(in);
		this.outputBufferLifetimeTarget = in.readInt();
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		this.attemptID.write(out);
		this.partitionID.write(out);
		out.writeInt(this.outputBufferLifetimeTarget);
	}
}
