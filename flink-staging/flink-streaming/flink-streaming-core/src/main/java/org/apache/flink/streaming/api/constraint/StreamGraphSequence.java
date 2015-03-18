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

import java.io.IOException;
import java.util.LinkedList;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/**
 * Represent a sequence of stream graph entities (e.g. vertices and edges).
 */
public class StreamGraphSequence extends LinkedList<StreamSequenceElement> implements IOReadableWritable {
	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeInt(size());
		for (StreamSequenceElement element : this) {
			element.write(out);
		}
	}

	@Override
	public void read(DataInputView in) throws IOException {
		int count = in.readInt();
		for (int i = 0; i < count; i++) {
			StreamSequenceElement element = new StreamSequenceElement();
			element.read(in);
			add(element);
		}
	}

	@Override
	public String toString() {
		StringBuilder stringBuilder = new StringBuilder();
		for (StreamSequenceElement sequenceElement : this) {
			if (sequenceElement.isVertex()) {
				stringBuilder.append(sequenceElement.getVertexID());
			} else {
				stringBuilder.append(" -> ");
			}
		}
		return stringBuilder.toString();
	}
}
