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

package org.apache.flink.streaming.statistics.taskmanager.qosmanager.buffers;

/**
 * Models an entry in a time-series of values.
 *
 * @author Bjoern Lohrmann
 *
 */
public class HistoryEntry<T> {
	private int entryIndex;

	private long timestamp;

	private T value;

	public HistoryEntry(int entryIndex, long timestamp, T bufferSize) {
		this.entryIndex = entryIndex;
		this.timestamp = timestamp;
		this.value = bufferSize;
	}

	public int getEntryIndex() {
		return this.entryIndex;
	}

	public long getTimestamp() {
		return this.timestamp;
	}

	public T getValue() {
		return this.value;
	}

}
