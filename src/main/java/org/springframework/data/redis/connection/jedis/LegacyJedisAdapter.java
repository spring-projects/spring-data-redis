/*
 * Copyright 2026-present the original author or authors.
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
package org.springframework.data.redis.connection.jedis;

import redis.clients.jedis.AbstractTransaction;
import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.UnifiedJedis;

/**
 * Adapter that wraps a {@link Jedis} instance to provide the {@link RedisClient} API.
 * <p>
 * This adapter enables {@link JedisConnection} to use the complete {@link RedisClient} API while
 * maintaining a single dedicated connection. Unlike pooled {@link RedisClient} implementations,
 * transactions and pipelines created by this adapter do not close the underlying connection.
 * <p>
 * This class is used for internal use only and would likely be removed once the legacy mode
 * is no longer supported and removed.
 *
 * @author Tihomir Mateev
 * @since 4.1
 * @see RedisClient
 * @see JedisConnection
 */
class LegacyJedisAdapter extends UnifiedJedis {

	private final Jedis jedis;

	/**
	 * Creates a new adapter wrapping the given {@link Jedis} instance.
	 *
	 * @param jedis the Jedis instance to wrap
	 */
	public LegacyJedisAdapter(Jedis jedis) {
		super(jedis.getConnection());
		this.jedis = jedis;
	}

	/**
	 * Returns the underlying {@link Jedis} instance.
	 *
	 * @return the wrapped Jedis instance
	 */
	public Jedis toJedis() {
		return jedis;
	}

	@Override
	public AbstractTransaction multi() {
		return new Transaction(jedis.getConnection(), true, false);
	}

	@Override
	public AbstractTransaction transaction(boolean doMulti) {
		return new Transaction(jedis.getConnection(), doMulti, false);
	}

	@Override
	public Pipeline pipelined() {
		return new Pipeline(jedis.getConnection(), false);
	}

	@Override
	public void subscribe(JedisPubSub jedisPubSub, String... channels) {
		jedisPubSub.proceed(jedis.getConnection(), channels);
	}

	@Override
	public void psubscribe(JedisPubSub jedisPubSub, String... patterns) {
		jedisPubSub.proceedWithPatterns(jedis.getConnection(), patterns);
	}

	@Override
	public void subscribe(BinaryJedisPubSub jedisPubSub, byte[]... channels) {
		jedisPubSub.proceed(jedis.getConnection(), channels);
	}

	@Override
	public void psubscribe(BinaryJedisPubSub jedisPubSub, byte[]... patterns) {
		jedisPubSub.proceedWithPatterns(jedis.getConnection(), patterns);
	}
}
