/*
 * Copyright 2018-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.connection.stream;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.time.Duration;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Options for reading messages from a Redis Stream.
 * 
 * @author Mark Paluch
 * @see 2.2
 */
@EqualsAndHashCode
@ToString
@Getter
public class StreamReadOptions {

	private static final StreamReadOptions EMPTY = new StreamReadOptions(null, null, false);

	private final @Nullable Long block;
	private final @Nullable Long count;
	private final boolean noack;

	private StreamReadOptions(@Nullable Long block, @Nullable Long count, boolean noack) {
		this.block = block;
		this.count = count;
		this.noack = noack;
	}

	/**
	 * Creates an empty {@link StreamReadOptions} instance.
	 *
	 * @return an empty {@link StreamReadOptions} instance.
	 */
	public static StreamReadOptions empty() {
		return EMPTY;
	}

	/**
	 * Disable auto-acknowledgement when reading in the context of a consumer group.
	 *
	 * @return {@link StreamReadOptions} with {@code noack} applied.
	 */
	public StreamReadOptions noack() {
		return new StreamReadOptions(block, count, true);
	}

	/**
	 * Use a blocking read and supply the {@link Duration timeout} after which the call will terminate if no message was
	 * read.
	 *
	 * @param timeout the timeout for the blocking read, must not be {@literal null} or negative.
	 * @return {@link StreamReadOptions} with {@code block} applied.
	 */
	public StreamReadOptions block(Duration timeout) {

		Assert.notNull(timeout, "Block timeout must not be null!");
		Assert.isTrue(!timeout.isNegative(), "Block timeout must not be negative!");

		return new StreamReadOptions(timeout.toMillis(), count, noack);
	}

	/**
	 * Limit the number of messages returned per stream.
	 *
	 * @param count the maximum number of messages to read.
	 * @return {@link StreamReadOptions} with {@code count} applied.
	 */
	public StreamReadOptions count(long count) {

		Assert.isTrue(count > 0, "Count must be greater or equal to zero!");

		return new StreamReadOptions(block, count, noack);
	}
}
