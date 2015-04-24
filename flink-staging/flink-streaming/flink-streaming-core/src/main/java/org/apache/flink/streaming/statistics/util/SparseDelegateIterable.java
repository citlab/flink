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

package org.apache.flink.streaming.statistics.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Convenient utility class to create a non-sparse iterable based on a sparse
 * iterator. A sparse iterator may return null values. This is achieved by
 * skipping the null values of the underlying iterator.
 *
 * @author Bjoern Lohrmann
 */
public class SparseDelegateIterable<T> implements Iterable<T> {

	private Iterator<T> sparseIterator;

	public SparseDelegateIterable(Iterator<T> sparseIterator) {
		this.sparseIterator = sparseIterator;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.lang.Iterable#iterator()
	 */
	@Override
	public Iterator<T> iterator() {
		return new Iterator<T>() {
			T current = null;

			@Override
			public boolean hasNext() {
				advanceCurrent();
				return this.current != null;
			}

			@Override
			public T next() {
				advanceCurrent();
				if (this.current != null) {
					T cur = this.current;
					this.current = null;
					return cur;
				} else {
					throw new NoSuchElementException();
				}
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}

			/**
			 * Advances the current element to the next non-null element.
			 */
			private void advanceCurrent() {
				while (this.current == null &&
						SparseDelegateIterable.this.sparseIterator.hasNext()) {
					this.current = SparseDelegateIterable.this.sparseIterator.next();
				}
			}
		};
	}
}
