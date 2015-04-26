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

package org.apache.flink.streaming.statistics.taskmanager.qosreporter;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.statistics.types.Tag;

import java.io.IOException;

/**
 * Instances of this class hold a timestamp tag that can be attached to an
 * {@link org.apache.flink.streaming.statistics.types.AbstractTaggableRecord}. It is used to
 * measure the latency of edges (channels) while a job runs.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public final class TimestampTag implements Tag {

	private long timestamp = 0L;

	public void setTimestamp(final long timestamp) {
		this.timestamp = timestamp;
	}

	public long getTimestamp() {
		return this.timestamp;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutputView out) throws IOException {
		out.writeLong(this.timestamp);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInputView in) throws IOException {
		this.timestamp = in.readLong();
	}
}
