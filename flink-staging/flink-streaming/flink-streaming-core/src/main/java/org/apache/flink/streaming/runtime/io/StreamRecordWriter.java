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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.io.network.api.serialization.RecordSerializer;
import org.apache.flink.runtime.io.network.api.writer.ChannelSelector;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.api.writer.RoundRobinChannelSelector;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.util.event.EventListener;
import org.apache.flink.streaming.statistics.message.action.SetOutputBufferLifetimeTargetEvent;
import org.apache.flink.streaming.statistics.taskmanager.qosreporter.listener.OutputGateQosReportingListener;
import org.apache.flink.streaming.statistics.types.AbstractTaggableRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class StreamRecordWriter<T extends IOReadableWritable> extends RecordWriter<T>
		implements EventListener<TaskEvent> {

	private OutputGateQosReportingListener qosCallback;

	private static final Logger LOG = LoggerFactory.getLogger(StreamRecordWriter.class);

	private long timeout;
	private boolean flushAlways = false;

	private OutputFlusher outputFlusher;

	public StreamRecordWriter(ResultPartitionWriter writer) {
		this(writer, new RoundRobinChannelSelector<T>(), 1000);
	}

	public StreamRecordWriter(ResultPartitionWriter writer, ChannelSelector<T> channelSelector) {
		this(writer, channelSelector, 1000);
	}

	public StreamRecordWriter(ResultPartitionWriter writer, ChannelSelector<T> channelSelector,
			long timeout) {
		super(writer, channelSelector);

		this.timeout = timeout;

		if (timeout == 0) {
			flushAlways = true;
		} else {
			this.outputFlusher = new OutputFlusher();
			outputFlusher.start();
			writer.subscribeToEvent(this, SetOutputBufferLifetimeTargetEvent.class);
		}
	}
	
	@Override
	public void onEvent(TaskEvent event) {
		if (event instanceof SetOutputBufferLifetimeTargetEvent) {
			SetOutputBufferLifetimeTargetEvent target = (SetOutputBufferLifetimeTargetEvent) event;
			this.timeout = target.getOutputBufferLifetimeTarget();
			LOG.debug("New auto flush timeout set: {}.", this.timeout);
		}
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

					if (qosCallback != null) {
						qosCallback.outputBufferAllocated(targetChannel);
					}
				}
			}
		}

		// TODO: channel[0] ?
		if (qosCallback != null && record instanceof AbstractTaggableRecord) {
			qosCallback.recordEmitted(channels[0], (AbstractTaggableRecord) record);

		} else if (qosCallback != null && record instanceof SerializationDelegate) {
			if (((SerializationDelegate) record).getInstance() instanceof AbstractTaggableRecord) {
				qosCallback.recordEmitted(channels[0],
						(AbstractTaggableRecord) ((SerializationDelegate)record).getInstance());
			}
		}
	}

	public void close() {
		try {
			if (outputFlusher != null) {
				outputFlusher.terminate();
				outputFlusher.join();
			}

			flush();
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			// Do nothing here
		}
	}

	private class OutputFlusher extends Thread {
		private volatile boolean running = true;

		public void terminate() {
			running = false;
		}

		@Override
		public void run() {
			while (running) {
				try {
					flush();
					Thread.sleep(timeout);
				} catch (InterruptedException e) {
					// Do nothing here
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		}
	}
}
