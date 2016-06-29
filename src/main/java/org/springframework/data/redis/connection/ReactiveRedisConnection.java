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

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.springframework.beans.BeanUtils;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.data.domain.Range;

import lombok.Data;

/**
 * @author Christoph Strobl
 * @since 2.0
 */
public interface ReactiveRedisConnection extends Closeable {

	/**
	 * Get {@link ReactiveKeyCommands}.
	 *
	 * @return never {@literal null}.
	 */
	ReactiveKeyCommands keyCommands();

	/**
	 * Get {@link ReactiveStringCommands}.
	 *
	 * @return never {@literal null}.
	 */
	ReactiveStringCommands stringCommands();

	/**
	 * Get {@link ReactiveNumberCommands}
	 *
	 * @return never {@literal null}.
	 */
	ReactiveNumberCommands numberCommands();

	/**
	 * Get {@link ReactiveListCommands}.
	 *
	 * @return never {@literal null}.
	 */
	ReactiveListCommands listCommands();

	/**
	 * Get {@link ReactiveSetCommands}.
	 *
	 * @return never {@literal null}.
	 */
	ReactiveSetCommands setCommands();

	/**
	 * Get {@link ReactiveZSetCommands}.
	 *
	 * @return never {@literal null}.
	 */
	ReactiveZSetCommands zSetCommands();

	/**
	 * Get {@link ReactiveHashCommands}.
	 *
	 * @return
	 */
	ReactiveHashCommands hashCommands();

	/**
	 * Get {@link ReacktiveGeoCommands}
	 *
	 * @return never {@literal null}.
	 */
	ReactiveGeoCommands geoCommands();

	/**
	 * Get {@link ReactiveHyperLogLogCommands}.
	 *
	 * @return never {@literal null}.
	 */
	ReactiveHyperLogLogCommands hyperLogLogCommands();

	interface Command {

		ByteBuffer getKey();

		default String getName() {
			return getClass().getSimpleName().replace("Command", "").toUpperCase();
		}

		static <T extends Command> Builder<T> create(Class<T> type) {
			return new CommandBuilder<T>(type);
		}

		interface Builder<T extends Command> extends Consumer<Object> {

			default Builder<T> forKey(String key) {
				return forKey(key.getBytes(Charset.forName("UTF-8")));
			}

			default Builder<T> forKey(byte[] key) {
				return forKey(ByteBuffer.wrap(key));
			}

			default Builder<T> forKey(ByteBuffer key) {
				return forKey(() -> key);
			}

			Builder<T> forKey(Supplier<ByteBuffer> keySupplier);

			T build();
		}

		class CommandBuilder<T extends Command> implements Builder<T> {

			List<Object> argumentList = new ArrayList<>();

			Class<T> type;
			Supplier<ByteBuffer> key;

			public CommandBuilder(Class<T> type) {
				this.type = type;
			}

			public Builder<T> forKey(Supplier<ByteBuffer> key) {
				this.key = key;
				return this;
			}

			@Override
			public void accept(Object t) {
				argumentList.add(t);
			}

			@Override
			public T build() {

				try {
					T x = BeanUtils.instantiateClass(type);

					DirectFieldAccessor dfa = new DirectFieldAccessor(x);
					dfa.setPropertyValue("key", key);
					return x;

				} catch (IllegalArgumentException | SecurityException e) {
					throw new IllegalArgumentException(" ¯\\_(ツ)_/¯", e);
				}
			}
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	class KeyCommand implements Command {

		private ByteBuffer key;

		public KeyCommand(ByteBuffer key) {
			this.key = key;
		}

		@Override
		public ByteBuffer getKey() {
			return key;
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	class RangeCommand extends KeyCommand {

		Range<Long> range;

		public RangeCommand(ByteBuffer key, Range<Long> range) {

			super(key);
			this.range = range != null ? range : new Range<>(0L, Long.MAX_VALUE);
		}

		public static RangeCommand key(ByteBuffer key) {
			return new RangeCommand(key, null);
		}

		public RangeCommand within(Range<Long> range) {
			return new RangeCommand(getKey(), range);
		}

		public RangeCommand fromIndex(Long start) {
			return new RangeCommand(getKey(), new Range<>(start, range.getUpperBound()));
		}

		public RangeCommand toIndex(Long end) {
			return new RangeCommand(getKey(), new Range<>(range.getLowerBound(), end));
		}

		public Range<Long> getRange() {
			return range;
		}
	}

	@Data
	class CommandResponse<I, O> {

		private final I input;
		private final O output;
	}

	class BooleanResponse<I> extends CommandResponse<I, Boolean> {

		public BooleanResponse(I input, Boolean output) {
			super(input, output);
		}
	}

	class ByteBufferResponse<I> extends CommandResponse<I, ByteBuffer> {

		public ByteBufferResponse(I input, ByteBuffer output) {
			super(input, output);
		}
	}

	class MultiValueResponse<I, O> extends CommandResponse<I, List<O>> {

		public MultiValueResponse(I input, List<O> output) {
			super(input, output);
		}
	}

	class NumericResponse<I, O extends Number> extends CommandResponse<I, O> {

		public NumericResponse(I input, O output) {
			super(input, output);
		}
	}

}
