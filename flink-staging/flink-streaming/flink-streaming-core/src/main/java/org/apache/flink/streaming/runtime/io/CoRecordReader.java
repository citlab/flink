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

import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.network.api.reader.AbstractReader;
import org.apache.flink.runtime.io.network.api.reader.MutableRecordReader;
import org.apache.flink.runtime.io.network.api.serialization.AdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.UnionInputGate;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.util.event.EventListener;
import org.apache.flink.streaming.runtime.tasks.StreamingSuperstep;
import org.apache.flink.streaming.statistics.taskmanager.qosreporter.QosReportingReader;
import org.apache.flink.streaming.statistics.taskmanager.qosreporter.listener.InputGateQosReportingListener;
import org.apache.flink.streaming.statistics.types.TimeStampedRecord;

/**
 * A CoRecordReader wraps {@link MutableRecordReader}s of two different input
 * types to read records effectively.
 */
@SuppressWarnings("rawtypes")
public class CoRecordReader<T1 extends IOReadableWritable, T2 extends IOReadableWritable> extends
		AbstractReader implements EventListener<InputGate>, StreamingReader, QosReportingReader {

	private final InputGate bufferReader1;

	private final InputGate bufferReader2;

	private final LinkedBlockingDeque<Integer> availableRecordReaders = new LinkedBlockingDeque<Integer>();

	private LinkedList<Integer> processed = new LinkedList<Integer>();

	private AdaptiveSpanningRecordDeserializer[] reader1RecordDeserializers;

	private RecordDeserializer<T1> reader1currentRecordDeserializer;

	private AdaptiveSpanningRecordDeserializer[] reader2RecordDeserializers;

	private RecordDeserializer<T2> reader2currentRecordDeserializer;

	// 0 => none, 1 => reader (T1), 2 => reader (T2)
	private int currentReaderIndex;

	private boolean hasRequestedPartitions;

	protected CoBarrierBuffer barrierBuffer1;
	protected CoBarrierBuffer barrierBuffer2;

	private int reader1currentRecordDeserializerIndex;
	private int reader2currentRecordDeserializerIndex;

	private InputGateQosReportingListener qosCallback1;
	private InputGateQosReportingListener qosCallback2;

	private long bufferInterarrivalTimeNanos1;
	private long bufferInterarrivalTimeNanos2;

	public CoRecordReader(InputGate inputgate1, InputGate inputgate2) {
		super(new UnionInputGate(inputgate1, inputgate2));

		this.bufferReader1 = inputgate1;
		this.bufferReader2 = inputgate2;

		this.reader1RecordDeserializers = new AdaptiveSpanningRecordDeserializer[inputgate1
				.getNumberOfInputChannels()];
		this.reader2RecordDeserializers = new AdaptiveSpanningRecordDeserializer[inputgate2
				.getNumberOfInputChannels()];

		for (int i = 0; i < reader1RecordDeserializers.length; i++) {
			reader1RecordDeserializers[i] = new AdaptiveSpanningRecordDeserializer<T1>();
		}

		for (int i = 0; i < reader2RecordDeserializers.length; i++) {
			reader2RecordDeserializers[i] = new AdaptiveSpanningRecordDeserializer<T2>();
		}

		inputgate1.registerListener(this);
		inputgate2.registerListener(this);

		barrierBuffer1 = new CoBarrierBuffer(inputgate1, this);
		barrierBuffer2 = new CoBarrierBuffer(inputgate2, this);

		barrierBuffer1.setOtherBarrierBuffer(barrierBuffer2);
		barrierBuffer2.setOtherBarrierBuffer(barrierBuffer1);
	}

	public void requestPartitionsOnce() throws IOException, InterruptedException {
		if (!hasRequestedPartitions) {
			bufferReader1.requestPartitions();
			bufferReader2.requestPartitions();

			hasRequestedPartitions = true;
		}
	}

	@SuppressWarnings("unchecked")
	protected int getNextRecord(T1 target1, T2 target2) throws IOException, InterruptedException {

		requestPartitionsOnce();

		while (true) {
			if (currentReaderIndex == 0) {
				if ((bufferReader1.isFinished() && bufferReader2.isFinished())) {
					return 0;
				}

				currentReaderIndex = getNextReaderIndexBlocking();

			}

			if (currentReaderIndex == 1) {

				if (qosCallback1 != null) {
					qosCallback1.tryingToReadRecord();
				}

				while (true) {
					if (reader1currentRecordDeserializer != null) {
						RecordDeserializer.DeserializationResult result = reader1currentRecordDeserializer
								.getNextRecord(target1);

						if (result.isBufferConsumed()) {
							reader1currentRecordDeserializer.getCurrentBuffer().recycle();
							reader1currentRecordDeserializer = null;

							currentReaderIndex = 0;

							if (qosCallback1 != null) {
								qosCallback1.inputBufferConsumed(reader1currentRecordDeserializerIndex, bufferInterarrivalTimeNanos1);
								bufferInterarrivalTimeNanos1 = -1;
							}
						}

						if (result.isFullRecord()) {
							if (qosCallback1 != null && target1 instanceof TimeStampedRecord) {
								qosCallback1.recordReceived(reader1currentRecordDeserializerIndex, (TimeStampedRecord) target1);

							} else if (qosCallback1 != null
									&& target1 instanceof DeserializationDelegate
									&& ((DeserializationDelegate) target1).getInstance() instanceof TimeStampedRecord) {

								qosCallback1.recordReceived(reader1currentRecordDeserializerIndex,
										(TimeStampedRecord) ((DeserializationDelegate) target1).getInstance());
							}
							return 1;
						}
					} else {

						final BufferOrEvent boe = barrierBuffer1.getNextNonBlocked();

						if (boe.isBuffer()) {
							reader1currentRecordDeserializerIndex = boe.getChannelIndex();
							reader1currentRecordDeserializer = reader1RecordDeserializers[reader1currentRecordDeserializerIndex];
							reader1currentRecordDeserializer.setNextBuffer(boe.getBuffer());
							bufferInterarrivalTimeNanos1 = boe.getBufferInterarrivalTimeNanos();
						} else if (boe.getEvent() instanceof StreamingSuperstep) {
							barrierBuffer1.processSuperstep(boe);
							currentReaderIndex = 0;

							break;
						} else if (handleEvent(boe.getEvent())) {
							currentReaderIndex = 0;

							break;
						}
					}
				}
			} else if (currentReaderIndex == 2) {

				if (qosCallback2 != null) {
					qosCallback2.tryingToReadRecord();
				}

				while (true) {
					if (reader2currentRecordDeserializer != null) {
						RecordDeserializer.DeserializationResult result = reader2currentRecordDeserializer
								.getNextRecord(target2);

						if (result.isBufferConsumed()) {
							reader2currentRecordDeserializer.getCurrentBuffer().recycle();
							reader2currentRecordDeserializer = null;

							currentReaderIndex = 0;


							if (qosCallback2 != null) {
								qosCallback2.inputBufferConsumed(reader2currentRecordDeserializerIndex, bufferInterarrivalTimeNanos2);
								bufferInterarrivalTimeNanos2 = -1;
							}
						}

						if (result.isFullRecord()) {
							if (qosCallback2 != null && target2 instanceof TimeStampedRecord) {
								qosCallback2.recordReceived(reader2currentRecordDeserializerIndex, (TimeStampedRecord) target2);

							} else if (qosCallback2 != null
									&& target2 instanceof DeserializationDelegate
									&& ((DeserializationDelegate) target2).getInstance() instanceof TimeStampedRecord) {

								qosCallback2.recordReceived(reader2currentRecordDeserializerIndex,
										(TimeStampedRecord) ((DeserializationDelegate) target2).getInstance());
							}
							return 2;
						}
					} else {
						final BufferOrEvent boe = barrierBuffer2.getNextNonBlocked();

						if (boe.isBuffer()) {
							reader2currentRecordDeserializerIndex = boe.getChannelIndex();
							reader2currentRecordDeserializer = reader2RecordDeserializers[reader2currentRecordDeserializerIndex];
							reader2currentRecordDeserializer.setNextBuffer(boe.getBuffer());
							bufferInterarrivalTimeNanos2 = boe.getBufferInterarrivalTimeNanos();
						} else if (boe.getEvent() instanceof StreamingSuperstep) {
							barrierBuffer2.processSuperstep(boe);
							currentReaderIndex = 0;

							break;
						} else if (handleEvent(boe.getEvent())) {
							currentReaderIndex = 0;

							break;
						}
					}
				}
			} else {
				throw new IllegalStateException("Bug: unexpected current reader index.");
			}
		}
	}

	protected int getNextReaderIndexBlocking() throws InterruptedException {

		Integer nextIndex = 0;

		while (processed.contains(nextIndex = availableRecordReaders.take())) {
			processed.remove(nextIndex);
		}

		if (nextIndex == 1) {
			if (barrierBuffer1.isAllBlocked()) {
				availableRecordReaders.addFirst(1);
				processed.add(2);
				return 2;
			} else {
				return 1;
			}
		} else {
			if (barrierBuffer2.isAllBlocked()) {
				availableRecordReaders.addFirst(2);
				processed.add(1);
				return 1;
			} else {
				return 2;
			}

		}

	}

	// ------------------------------------------------------------------------
	// Data availability notifications
	// ------------------------------------------------------------------------

	@Override
	public void onEvent(InputGate bufferReader) {
		addToAvailable(bufferReader);
	}

	protected void addToAvailable(InputGate bufferReader) {
		if (bufferReader == bufferReader1) {
			availableRecordReaders.add(1);
		} else if (bufferReader == bufferReader2) {
			availableRecordReaders.add(2);
		}
	}

	public void clearBuffers() {
		for (RecordDeserializer<?> deserializer : reader1RecordDeserializers) {
			Buffer buffer = deserializer.getCurrentBuffer();
			if (buffer != null && !buffer.isRecycled()) {
				buffer.recycle();
			}
		}
		for (RecordDeserializer<?> deserializer : reader2RecordDeserializers) {
			Buffer buffer = deserializer.getCurrentBuffer();
			if (buffer != null && !buffer.isRecycled()) {
				buffer.recycle();
			}
		}
	}

	private class CoBarrierBuffer extends BarrierBuffer {

		private CoBarrierBuffer otherBuffer;

		public CoBarrierBuffer(InputGate inputGate, AbstractReader reader) {
			super(inputGate, reader);
		}

		public void setOtherBarrierBuffer(CoBarrierBuffer other) {
			this.otherBuffer = other;
		}

		@Override
		protected void actOnAllBlocked() {
			if (otherBuffer.isAllBlocked()) {
				super.actOnAllBlocked();
				otherBuffer.releaseBlocks();
			}
		}

	}

	public void cleanup() throws IOException {
		try {
			barrierBuffer1.cleanup();
		} finally {
			barrierBuffer2.cleanup();
		}

	}

	@Override
	public InputGateQosReportingListener getQosCallback(int index) {
		if (index == 0) {
			return qosCallback1;
		} else if (index == 1) {
			return qosCallback2;
		} else {
			throw new IllegalArgumentException("Only two InputGate's available with index = 0 and index = 1.");
		}
	}

	@Override
	public void setQosCallback(InputGateQosReportingListener qosCallback, int index) {
		if (index == 0) {
			this.qosCallback1 = qosCallback;
			this.bufferInterarrivalTimeNanos1 = -1;
		} else if (index == 1) {
			this.qosCallback2 = qosCallback;
			this.bufferInterarrivalTimeNanos2 = -1;
		} else {
			throw new IllegalArgumentException("Only two InputGate's available with index = 0 and index = 1.");
		}
	}
}
