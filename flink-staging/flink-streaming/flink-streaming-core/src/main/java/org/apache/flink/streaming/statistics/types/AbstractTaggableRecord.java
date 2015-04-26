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

package org.apache.flink.streaming.statistics.types;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.StringValue;
import org.apache.flink.types.Value;
import org.apache.flink.util.StringUtils;


import java.io.IOException;

public abstract class AbstractTaggableRecord implements Value {

	private Tag tag = null;

	public void setTag(final Tag tag) {
		this.tag = tag;
	}

	public Tag getTag() {

		return this.tag;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutputView out) throws IOException {

		if (this.tag == null) {
			out.writeBoolean(false);
		} else {
			out.writeBoolean(true);
			StringValue.writeString(this.tag.getClass().getName(), out);
			this.tag.write(out);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void read(final DataInputView in) throws IOException {

		if (in.readBoolean()) {
			final String tagType = StringValue.readString(in);
			Class<Tag> clazz = null;
			try {
				clazz = (Class<Tag>) Class.forName(tagType);
			} catch (ClassNotFoundException e) {
				throw new IOException(StringUtils.stringifyException(e));
			}

			try {
				this.tag = clazz.newInstance();
			} catch (Exception e) {
				throw new IOException(StringUtils.stringifyException(e));
			}
			this.tag.read(in);
		}
	}
}
