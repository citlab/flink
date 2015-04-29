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

package org.apache.flink.streaming.runtime.io;

import java.io.IOException;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.network.api.serialization.RecordSerializer;
import org.apache.flink.runtime.io.network.api.writer.ChannelSelector;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.streaming.statistics.taskmanager.qosreporter.listener.OutputGateQosReportingListener;
import org.apache.flink.streaming.statistics.types.AbstractTaggableRecord;

public class QosReportingRecordWriter<T extends IOReadableWritable> extends RecordWriter<T> {
	private OutputGateQosReportingListener qosCallback;

	public QosReportingRecordWriter(ResultPartitionWriter writer) {
		super(writer);
	}

	public QosReportingRecordWriter(ResultPartitionWriter writer, ChannelSelector<T> channelSelector) {
		super(writer, channelSelector);
	}


	public OutputGateQosReportingListener getQosCallback() {
		return qosCallback;
	}

	public void setQosCallback(OutputGateQosReportingListener qosCallback) {
		this.qosCallback = qosCallback;
	}

	@Override
	public void emit(T record) throws IOException, InterruptedException {
		int[] channels = channelSelector.selectChannels(record, numChannels);
		for (int targetChannel : channels) {
			// serialize with corresponding serializer and send full buffer
			RecordSerializer<T> serializer = serializers[targetChannel];

			synchronized (serializer) {
				RecordSerializer.SerializationResult result = serializer.addRecord(record);
				while (result.isFullBuffer()) {
					Buffer buffer = serializer.getCurrentBuffer();

					if (buffer != null) {
						writer.writeBuffer(buffer, targetChannel);

						if (qosCallback != null) {
							qosCallback.outputBufferSent(targetChannel, writer.getPartition().getTotalNumberOfBytes());
						}

					}

					buffer = writer.getBufferProvider().requestBufferBlocking();
					result = serializer.setNextBuffer(buffer);
				}
			}
		}

		if (qosCallback != null && record instanceof AbstractTaggableRecord) {
			qosCallback.recordEmitted(channels[0], (AbstractTaggableRecord) record);
		}
	}
}
