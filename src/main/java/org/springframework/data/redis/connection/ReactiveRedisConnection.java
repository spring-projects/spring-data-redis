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

import lombok.Data;
import reactor.core.publisher.Mono;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.List;

import org.springframework.data.domain.Range;
import org.springframework.data.domain.Range.Bound;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Redis connection using reactive infrastructure declaring entry points for reactive command execution.
 * <p>
 * {@link ReactiveRedisConnection} is typically implemented by a stateful object that requires to be {@link #close()
 * closed} once it is no longer required.
 * <p>
 * Commands can be either executed by passing plain arguments like {@code key}, {@code value} or wrapped inside a
 * command stream. Streaming command execution accepts {@link org.reactivestreams.Publisher} of a particular
 * {@link Command}. Commands are executed at the time their emission.
 * <p>
 * Arguments are binary-safe by using {@link ByteBuffer} arguments. Expect {@link ByteBuffer} to be consumed by
 * {@link ReactiveRedisConnection} invocation or during execution. Any {@link ByteBuffer} used as method parameter
 * should not be altered after invocation.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 * @see Command
 * @see CommandResponse
 * @see KeyCommand
 */
public interface ReactiveRedisConnection extends Closeable {

	/*
	 * (non-Javadoc)
	 * @see java.io.Closeable#close()
	 */
	@Override
	default void close() {
		closeLater().block();
	}

	/**
	 * Asynchronously close the connection and release associated resources.
	 *
	 * @return the {@link Mono} signaling when done.
	 */
	Mono<Void> closeLater();

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
	 * @return never {@literal null}.
	 */
	ReactiveHashCommands hashCommands();

	/**
	 * Get {@link ReactiveGeoCommands}
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

	/**
	 * Get {@link ReactivePubSubCommands}.
	 *
	 * @return never {@literal null}.
	 * @since 2.1
	 */
	ReactivePubSubCommands pubSubCommands();

	/**
	 * Get {@link ReactiveScriptingCommands}.
	 *
	 * @return never {@literal null}.
	 */
	ReactiveScriptingCommands scriptingCommands();

	/**
	 * Get {@link ReactiveServerCommands}.
	 *
	 * @return never {@literal null}.
	 */
	ReactiveServerCommands serverCommands();

	/**
	 * Test connection.
	 *
	 * @return {@link Mono} wrapping server response message - usually {@literal PONG}.
	 * @see <a href="http://redis.io/commands/ping">Redis Documentation: PING</a>
	 */
	Mono<String> ping();

	/**
	 * Base interface for Redis commands executed with a reactive infrastructure.
	 *
	 * @author Christoph Strobl
	 * @author Mark Paluch
	 */
	interface Command {

		/**
		 * @return the key related to this command.
		 */
		@Nullable
		ByteBuffer getKey();

		/**
		 * @return command name as {@link String}.
		 */
		default String getName() {
			return getClass().getSimpleName().replace("Command", "").toUpperCase();
		}
	}

	/**
	 * {@link Command} for key-bound operations.
	 *
	 * @author Christoph Strobl
	 */
	class KeyCommand implements Command {

		private @Nullable ByteBuffer key;

		/**
		 * Creates a new {@link KeyCommand} given a {@code key}.
		 *
		 * @param key can be {@literal null}.
		 */
		public KeyCommand(@Nullable ByteBuffer key) {
			this.key = key;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.Command#getKey()
		 */
		@Override
		public ByteBuffer getKey() {
			return key;
		}
	}

	/**
	 * {@link Command} for key-bound scan operations like {@code SCAN}, {@code HSCAN}, {@code SSCAN} and {@code
	 * ZSCAN}.
	 *
	 * @author Mark Paluch
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	class KeyScanCommand extends KeyCommand {

		private final ScanOptions options;

		private KeyScanCommand(@Nullable ByteBuffer key, ScanOptions options) {

			super(key);

			Assert.notNull(options, "ScanOptions must not be null!");
			this.options = options;
		}

		/**
		 * Creates a new {@link KeyScanCommand} given a {@code key}.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link KeyScanCommand} for {@code key}.
		 */
		public static KeyScanCommand key(ByteBuffer key) {
			return new KeyScanCommand(key, ScanOptions.NONE);
		}

		/**
		 * Applies {@link ScanOptions}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param options must not be {@literal null}.
		 * @return a new {@link KeyScanCommand} with {@link ScanOptions} applied.
		 */
		public KeyScanCommand withOptions(ScanOptions options) {
			return new KeyScanCommand(getKey(), options);
		}

		/**
		 * Get the {@link ScanOptions} to apply.
		 *
		 * @return never {@literal null}.
		 */
		public ScanOptions getOptions() {
			return options;
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	class RangeCommand extends KeyCommand {

		Range<Long> range;

		/**
		 * Creates a new {@link RangeCommand} given a {@code key} and {@link Range}.
		 *
		 * @param key must not be {@literal null}.
		 * @param range must not be {@literal null}.
		 */
		private RangeCommand(ByteBuffer key, Range<Long> range) {

			super(key);
			this.range = range;
		}

		/**
		 * Creates a new {@link RangeCommand} given a {@code key}.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link RangeCommand} for {@code key}.
		 */
		public static RangeCommand key(ByteBuffer key) {
			return new RangeCommand(key, Range.unbounded());
		}

		/**
		 * Applies a {@link Range}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param range must not be {@literal null}.
		 * @return a new {@link RangeCommand} with {@link Range} applied.
		 */
		public RangeCommand within(Range<Long> range) {

			Assert.notNull(range, "Range must not be null!");

			return new RangeCommand(getKey(), range);
		}

		/**
		 * Applies a lower bound to the {@link Range}. Constructs a new command instance with all previously configured
		 * properties.
		 *
		 * @param start
		 * @return a new {@link RangeCommand} with the lower bound applied.
		 */
		public RangeCommand fromIndex(long start) {
			return new RangeCommand(getKey(), Range.of(Bound.inclusive(start), range.getUpperBound()));
		}

		/**
		 * Applies an upper bound to the {@link Range}. Constructs a new command instance with all previously configured
		 * properties.
		 *
		 * @param end
		 * @return a new {@link RangeCommand} with the upper bound applied.
		 */
		public RangeCommand toIndex(long end) {
			return new RangeCommand(getKey(), Range.of(range.getLowerBound(), Bound.inclusive(end)));
		}

		/**
		 * @return the {@link Range}.
		 */
		public Range<Long> getRange() {
			return range;
		}
	}

	/**
	 * Base class for command responses.
	 *
	 * @param <I> command input type.
	 * @param <O> command output type.
	 */
	@Data
	class CommandResponse<I, O> {

		private final I input;
		private final @Nullable O output;

		/**
		 * @return {@literal true} if the response is present. An absent {@link CommandResponse} maps to Redis
		 *         {@literal (nil)}.
		 */
		public boolean isPresent() {
			return true;
		}
	}

	/**
	 * {@link CommandResponse} implementation for {@link Boolean} responses.
	 */
	class BooleanResponse<I> extends CommandResponse<I, Boolean> {

		public BooleanResponse(I input, Boolean output) {
			super(input, output);
		}
	}

	/**
	 * {@link CommandResponse} implementation for {@link ByteBuffer} responses.
	 */
	class ByteBufferResponse<I> extends CommandResponse<I, ByteBuffer> {

		public ByteBufferResponse(I input, @Nullable ByteBuffer output) {
			super(input, output);
		}
	}

	/**
	 * {@link CommandResponse} implementation for {@link ByteBuffer} responses for absent keys.
	 */
	class AbsentByteBufferResponse<I> extends ByteBufferResponse<I> {

		private final static ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new byte[0]);

		public AbsentByteBufferResponse(I input) {
			super(input, EMPTY_BYTE_BUFFER);
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.CommandResponse#isPresent()
		 */
		@Override
		public boolean isPresent() {
			return false;
		}
	}

	/**
	 * {@link CommandResponse} implementation for {@link List} responses.
	 */
	class MultiValueResponse<I, O> extends CommandResponse<I, List<O>> {

		public MultiValueResponse(I input, List<O> output) {
			super(input, output);
		}
	}

	/**
	 * {@link CommandResponse} implementation for {@link Number numeric} responses.
	 */
	class NumericResponse<I, O extends Number> extends CommandResponse<I, O> {

		public NumericResponse(I input, O output) {
			super(input, output);
		}
	}
}
