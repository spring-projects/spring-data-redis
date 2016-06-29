/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.connection;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.reactivestreams.Publisher;
import org.springframework.data.redis.connection.ReactiveRedisConnection.BooleanResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.Command;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.util.Assert;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Christoph Strobl
 * @since 2.0
 */
public interface ReactiveHyperLogLogCommands {

	/**
	 * @author Christoph Strobl
	 */
	class PfAddCommand extends KeyCommand {

		private final List<ByteBuffer> values;

		private PfAddCommand(ByteBuffer key, List<ByteBuffer> values) {

			super(key);
			this.values = values;
		}

		public static PfAddCommand value(ByteBuffer value) {
			return values(Collections.singletonList(value));
		}

		public static PfAddCommand values(List<ByteBuffer> values) {
			return new PfAddCommand(null, new ArrayList<>(values));
		}

		public PfAddCommand to(ByteBuffer key) {
			return new PfAddCommand(key, values);
		}

		public List<ByteBuffer> getValues() {
			return values;
		}
	}

	/**
	 * Adds given {@literal value} to the HyperLogLog stored at given {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> pfAdd(ByteBuffer key, ByteBuffer value) {

		Assert.notNull(value, "value must not be null");

		return pfAdd(key, Collections.singletonList(value));
	}

	/**
	 * Adds given {@literal values} to the HyperLogLog stored at given {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param values must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> pfAdd(ByteBuffer key, List<ByteBuffer> values) {

		Assert.notNull(key, "key must not be null");
		Assert.notNull(values, "values must not be null");

		return pfAdd(Mono.just(PfAddCommand.values(values).to(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Adds given {@literal values} to the HyperLogLog stored at given {@literal key}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<NumericResponse<PfAddCommand, Long>> pfAdd(Publisher<PfAddCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	class PfCountCommand implements Command {

		private final List<ByteBuffer> keys;

		private PfCountCommand(List<ByteBuffer> keys) {

			super();
			this.keys = keys;
		}

		public static PfCountCommand valueIn(ByteBuffer key) {
			return valuesIn(Collections.singletonList(key));
		}

		public static PfCountCommand valuesIn(List<ByteBuffer> keys) {
			return new PfCountCommand(keys);
		}

		public List<ByteBuffer> getKeys() {
			return keys;
		}

		@Override
		public ByteBuffer getKey() {
			return null;
		}

	}

	/**
	 * Return the approximated cardinality of the structures observed by the HyperLogLog at {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> pfCount(ByteBuffer key) {

		Assert.notNull(key, "key must not be null");

		return pfCount(Collections.singletonList(key));
	}

	/**
	 * Return the approximated cardinality of the structures observed by the HyperLogLog at {@literal key(s)}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> pfCount(List<ByteBuffer> keys) {

		Assert.notNull(keys, "keys must not be null");

		return pfCount(Mono.just(PfCountCommand.valuesIn(keys))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Return the approximated cardinality of the structures observed by the HyperLogLog at {@literal key(s)}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<NumericResponse<PfCountCommand, Long>> pfCount(Publisher<PfCountCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	class PfMergeCommand extends KeyCommand {

		private final List<ByteBuffer> sourceKeys;

		private PfMergeCommand(ByteBuffer key, List<ByteBuffer> sourceKeys) {

			super(key);
			this.sourceKeys = sourceKeys;
		}

		public static PfMergeCommand valuesIn(List<ByteBuffer> sourceKeys) {
			return new PfMergeCommand(null, sourceKeys);
		}

		public PfMergeCommand into(ByteBuffer destinationKey) {
			return new PfMergeCommand(destinationKey, sourceKeys);
		}

		public List<ByteBuffer> getSourceKeys() {
			return sourceKeys;
		}
	}

	/**
	 * Merge N different HyperLogLogs at {@literal sourceKeys} into a single {@literal destinationKey}.
	 *
	 * @param destinationKey must not be {@literal null}.
	 * @param sourceKeys must not be {@literal null}.
	 * @return
	 */
	default Mono<Boolean> pfMerge(ByteBuffer destinationKey, List<ByteBuffer> sourceKeys) {

		Assert.notNull(destinationKey, "destinationKey must not be null");
		Assert.notNull(sourceKeys, "sourceKeys must not be null");

		return pfMerge(Mono.just(PfMergeCommand.valuesIn(sourceKeys).into(destinationKey))).next()
				.map(BooleanResponse::getOutput);
	}

	/**
	 * Merge N different HyperLogLogs at {@literal sourceKeys} into a single {@literal destinationKey}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<BooleanResponse<PfMergeCommand>> pfMerge(Publisher<PfMergeCommand> commands);

}
