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

package org.apache.flink.streaming.api;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.constraint.StreamGraphConstraint;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.test.testdata.WordCountData;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.Iterator;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class DataStreamTest {

	/**
	 * Generate a graph with two sequences => two constraints.
	 *
	 * source -> mapA --------> coFlatMap
	 *        -> mapB1 -> mapB2
	 */
	@Test
	public void testConstraint() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<String> source = env.fromElements(WordCountData.STREAMING_COUNTS_AS_TUPLES);

		source.beginLatencyConstraint("testConstraint", 100);

		DataStream<String> mapA = source
				.map(new MapFunction<String, String>() {
					@Override
					public String map(String value) throws Exception {
						return "mapA";
					}
				});

		DataStream<String> mapB1 = source
				.map(new MapFunction<String, String>() {
					@Override
					public String map(String value) throws Exception {
						return "mapB1";
					}
				});

		DataStream<String> mapB2 = mapB1
				.map(new MapFunction<String, String>() {
					@Override
					public String map(String value) throws Exception {
						return "mapB2";
					}
				});

		SingleOutputStreamOperator<String, ?> coFlatMap = mapA
				.connect(mapB2)
				.flatMap(new CoFlatMapFunction<String, String, String>() {
					@Override
					public void flatMap1(String value, Collector<String> out) throws Exception {
						out.collect("coFlatMap");
					}

					@Override
					public void flatMap2(String value, Collector<String> out) throws Exception {
						out.collect("coFlatMap");
					}
				});

		coFlatMap.finishLatencyConstraint();

		StreamGraph streamGraph = env.getStreamGraph();

		streamGraph.calculateConstraints();
		Set<StreamGraphConstraint> constraints = streamGraph.calculateConstraints();
		assertEquals(2, constraints.size());

		Iterator<StreamGraphConstraint> iterator = constraints.iterator();
		StreamGraphConstraint constraintA = iterator.next();
		assertEquals(100, constraintA.getLatencyConstraintInMillis());
		assertEquals(5, constraintA.getSequence().size());
		assertEquals("testConstraint (1)", constraintA.getName("1", "2"));

		StreamGraphConstraint constraintB = iterator.next();
		assertEquals(100, constraintB.getLatencyConstraintInMillis());
		assertEquals(7, constraintB.getSequence().size());
		assertEquals("testConstraint (2)", constraintB.getName("1", "2"));
	}
}
