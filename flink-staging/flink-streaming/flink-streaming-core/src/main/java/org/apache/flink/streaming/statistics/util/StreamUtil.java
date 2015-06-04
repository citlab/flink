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

import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Provides convenience methods that wrap the sometimes bulky constructor calls
 * to other classes in this package.
 * 
 * @author Bjoern Lohrmann
 */
public class StreamUtil {
	
	private static ScheduledThreadPoolExecutor scheduledThreadPool = new ScheduledThreadPoolExecutor(2);
	
	public static ScheduledFuture<?> scheduledAtFixedRate(Runnable command,
			long initialDelay, long period, TimeUnit unit) {

		return scheduledThreadPool.scheduleAtFixedRate(command, initialDelay, period,
				unit);
	}

	public static <V> SparseDelegateIterable<V> toIterable(
			Iterator<V> sparseIterator) {
		return new SparseDelegateIterable<V>(sparseIterator);
	}

	public static <T> T[] appendToArrayAt(T[]  oldArray,
			Class<T> type,
			T toAppend) {	
		return setInArrayAt(oldArray, type, oldArray.length, toAppend);
	}
	
	@SuppressWarnings("unchecked")
	public static <T> T[] setInArrayAt(T[] oldArray, Class<T> type, int index,
			T value) {

		T[] ret = oldArray;
		if (ret.length <= index) {
			ret = (T[]) Array.newInstance(type, index + 1);
			System.arraycopy(oldArray, 0, ret, 0, oldArray.length);
		}

		ret[index] = value;
		return ret;
	}
	
	public static <T> AtomicReferenceArray<T[]>  createAtomicReferenceArrayOfEmptyArrays(
			Class<T> type,
			int noOfEmptyArrays) {
		
		AtomicReferenceArray<T[]> ret = new AtomicReferenceArray<T[]>(noOfEmptyArrays);
	
		@SuppressWarnings("unchecked")
		final T[] emptyArray = (T[]) Array.newInstance(type, 0);
		
		for (int i = 0; i < noOfEmptyArrays; i++) {
			ret.set(i, emptyArray);
		}
		
		return ret;
	}	
}
