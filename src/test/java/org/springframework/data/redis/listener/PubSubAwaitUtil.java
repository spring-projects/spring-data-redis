/*
 * Copyright 2020 the original author or authors.
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
package org.springframework.data.redis.listener;

import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.output.ArrayOutput;
import io.lettuce.core.output.IntegerOutput;

import java.util.List;
import java.util.concurrent.Callable;

import org.awaitility.Awaitility;

import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnection;

/**
 * Utility providing await support.
 * 
 * @author Mark Paluch
 */
class PubSubAwaitUtil {

	/**
	 * Run the {@link Runnable} and wait until the number of Pub/Sub patterns has increased in comparison to before
	 * running the callback.
	 * 
	 * @param connectionFactory
	 * @param runnable
	 */
	public static void runAndAwaitPatternSubscription(RedisConnectionFactory connectionFactory, Runnable runnable) {

		try (RedisConnection connection = connectionFactory.getConnection()) {

			Number before = numPat(connection);

			runnable.run();

			runAndAwait(runnable, () -> {

				Number after = numPat(connection);

				return after.longValue() > before.longValue();
			});
		}
	}

	/**
	 * Run the {@link Runnable} and wait until the number of channel subscribers has increased in comparison to before
	 * running the callback.
	 * 
	 * @param connectionFactory
	 * @param channel
	 * @param runnable
	 */
	public static void runAndAwaitChannelSubscription(RedisConnectionFactory connectionFactory, String channel,
			Runnable runnable) {

		try (RedisConnection connection = connectionFactory.getConnection()) {

			Number before = numSub(connection, channel);

			runnable.run();

			runAndAwait(runnable, () -> {

				Number after = numSub(connection, channel);

				return after.longValue() > before.longValue();
			});
		}
	}

	private static long numPat(RedisConnection connection) {

		if (connection instanceof LettuceConnection) {
			return ((Number) ((LettuceConnection) connection).execute("PUBSUB", new IntegerOutput<>(ByteArrayCodec.INSTANCE),
					"NUMPAT".getBytes())).longValue();
		}

		return ((Number) connection.execute("PUBSUB", "NUMPAT".getBytes())).longValue();
	}

	private static long numSub(RedisConnection connection, String channel) {

		List<?> pubsub;
		if (connection instanceof LettuceConnection) {
			pubsub = (List<?>) ((LettuceConnection) connection).execute("PUBSUB", new ArrayOutput<>(ByteArrayCodec.INSTANCE),
					"NUMSUB".getBytes(), channel.getBytes());
		} else {
			pubsub = (List<?>) connection.execute("PUBSUB", "NUMSUB".getBytes(), channel.getBytes());
		}

		return ((Number) pubsub.get(1)).longValue();
	}

	private static void runAndAwait(Runnable runnable, Callable<Boolean> predicate) {

		runnable.run();
		Awaitility.await().until(predicate);
	}
}
