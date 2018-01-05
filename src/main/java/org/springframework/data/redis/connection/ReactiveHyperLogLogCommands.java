/*
 * Copyright 2016-2018 the original author or authors.
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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.reactivestreams.Publisher;
import org.springframework.data.redis.connection.ReactiveRedisConnection.BooleanResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.Command;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.util.Assert;

/**
 * Redis HyperLogLog commands executed using reactive infrastructure.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
public interface ReactiveHyperLogLogCommands {

	/**
	 * {@code PFADD} command parameters.
	 *
	 * @author Christoph Strobl
	 * @see <a href="http://redis.io/commands/pfadd">Redis Documentation: PFADD</a>
	 */
	class PfAddCommand extends KeyCommand {

		private final List<ByteBuffer> values;

		private PfAddCommand(ByteBuffer key, List<ByteBuffer> values) {

			super(key);
			this.values = values;
		}

		/**
		 * Creates a new {@link PfAddCommand} given a {@link ByteBuffer value}.
		 *
		 * @param value must not be {@literal null}.
		 * @return a new {@link PfAddCommand} for {@link ByteBuffer value}.
		 */
		public static PfAddCommand value(ByteBuffer value) {

			Assert.notNull(value, "Value must not be null!");

			return values(Collections.singletonList(value));
		}

		/**
		 * Creates a new {@link PfAddCommand} given a {@link Collection} of {@link ByteBuffer values}.
		 *
		 * @param values must not be {@literal null}.
		 * @return a new {@link PfAddCommand} for {@link ByteBuffer key}.
		 */
		public static PfAddCommand values(Collection<ByteBuffer> values) {

			Assert.notNull(values, "Values must not be null!");

			return new PfAddCommand(null, new ArrayList<>(values));
		}

		/**
		 * Applies the {@literal key}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link PfAddCommand} with {@literal key} applied.
		 */
		public PfAddCommand to(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null!");

			return new PfAddCommand(key, values);
		}

		/**
		 * @return
		 */
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
	 * @see <a href="http://redis.io/commands/pfadd">Redis Documentation: PFADD</a>
	 */
	default Mono<Long> pfAdd(ByteBuffer key, ByteBuffer value) {

		Assert.notNull(value, "Value must not be null!");

		return pfAdd(key, Collections.singletonList(value));
	}

	/**
	 * Adds given {@literal values} to the HyperLogLog stored at given {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param values must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/pfadd">Redis Documentation: PFADD</a>
	 */
	default Mono<Long> pfAdd(ByteBuffer key, Collection<ByteBuffer> values) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(values, "Values must not be null!");

		return pfAdd(Mono.just(PfAddCommand.values(values).to(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Adds given {@literal values} to the HyperLogLog stored at given {@literal key}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/pfadd">Redis Documentation: PFADD</a>
	 */
	Flux<NumericResponse<PfAddCommand, Long>> pfAdd(Publisher<PfAddCommand> commands);

	/**
	 * {@code PFCOUNT} command parameters.
	 *
	 * @author Christoph Strobl
	 * @see <a href="http://redis.io/commands/pfcount">Redis Documentation: PFCOUNT</a>
	 */
	class PfCountCommand implements Command {

		private final List<ByteBuffer> keys;

		private PfCountCommand(List<ByteBuffer> keys) {

			super();
			this.keys = keys;
		}

		/**
		 * Creates a new {@link PfCountCommand} given a {@link ByteBuffer key}.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link PfCountCommand} for {@link ByteBuffer key}.
		 */
		public static PfCountCommand valueIn(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null!");

			return valuesIn(Collections.singletonList(key));
		}

		/**
		 * Creates a new {@link PfCountCommand} given a {@link Collection} of {@literal keys}.
		 *
		 * @param keys must not be {@literal null}.
		 * @return a new {@link PfCountCommand} for {@literal keys}.
		 */
		public static PfCountCommand valuesIn(Collection<ByteBuffer> keys) {

			Assert.notNull(keys, "Keys must not be null!");

			return new PfCountCommand(new ArrayList<>(keys));
		}

		/**
		 * @return
		 */
		public List<ByteBuffer> getKeys() {
			return keys;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.Command#getKey()
		 */
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
	 * @see <a href="http://redis.io/commands/pfcount">Redis Documentation: PFCOUNT</a>
	 */
	default Mono<Long> pfCount(ByteBuffer key) {

		Assert.notNull(key, "Key must not be null!");

		return pfCount(Collections.singletonList(key));
	}

	/**
	 * Return the approximated cardinality of the structures observed by the HyperLogLog at {@literal key(s)}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/pfcount">Redis Documentation: PFCOUNT</a>
	 */
	default Mono<Long> pfCount(Collection<ByteBuffer> keys) {

		Assert.notNull(keys, "Keys must not be null!");

		return pfCount(Mono.just(PfCountCommand.valuesIn(keys))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Return the approximated cardinality of the structures observed by the HyperLogLog at {@literal key(s)}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/pfcount">Redis Documentation: PFCOUNT</a>
	 */
	Flux<NumericResponse<PfCountCommand, Long>> pfCount(Publisher<PfCountCommand> commands);

	/**
	 * {@code PFMERGE} command parameters.
	 *
	 * @author Christoph Strobl
	 * @see <a href="http://redis.io/commands/pfmerge">Redis Documentation: PFMERGE</a>
	 */
	class PfMergeCommand extends KeyCommand {

		private final List<ByteBuffer> sourceKeys;

		private PfMergeCommand(ByteBuffer key, List<ByteBuffer> sourceKeys) {

			super(key);
			this.sourceKeys = sourceKeys;
		}

		/**
		 * Creates a new {@link PfMergeCommand} given a {@link Collection} of {@literal sourceKeys}.
		 *
		 * @param sourceKeys must not be {@literal null}.
		 * @return a new {@link PfMergeCommand} for {@literal sourceKeys}.
		 */
		public static PfMergeCommand valuesIn(Collection<ByteBuffer> sourceKeys) {

			Assert.notNull(sourceKeys, "Source keys must not be null!");

			return new PfMergeCommand(null, new ArrayList<>(sourceKeys));
		}

		/**
		 * Applies the {@literal destinationKey}. Constructs a new command instance with all previously configured
		 * properties.
		 *
		 * @param destinationKey must not be {@literal null}.
		 * @return a new {@link PfMergeCommand} with {@literal destinationKey} applied.
		 */
		public PfMergeCommand into(ByteBuffer destinationKey) {

			Assert.notNull(destinationKey, "Destination key must not be null!");

			return new PfMergeCommand(destinationKey, sourceKeys);
		}

		/**
		 * @return
		 */
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
	 * @see <a href="http://redis.io/commands/pfmerge">Redis Documentation: PFMERGE</a>
	 */
	default Mono<Boolean> pfMerge(ByteBuffer destinationKey, Collection<ByteBuffer> sourceKeys) {

		Assert.notNull(destinationKey, "DestinationKey must not be null!");
		Assert.notNull(sourceKeys, "SourceKeys must not be null!");

		return pfMerge(Mono.just(PfMergeCommand.valuesIn(sourceKeys).into(destinationKey))).next()
				.map(BooleanResponse::getOutput);
	}

	/**
	 * Merge N different HyperLogLogs at {@literal sourceKeys} into a single {@literal destinationKey}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/pfmerge">Redis Documentation: PFMERGE</a>
	 */
	Flux<BooleanResponse<PfMergeCommand>> pfMerge(Publisher<PfMergeCommand> commands);
}
