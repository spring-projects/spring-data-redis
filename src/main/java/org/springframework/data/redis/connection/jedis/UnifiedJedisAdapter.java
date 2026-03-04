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
import redis.clients.jedis.Transaction;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;
import redis.clients.jedis.resps.Tuple;

import java.util.Map.Entry;

/**
 * Adapter that wraps a {@link Jedis} instance to provide the {@link UnifiedJedis} API.
 * <p>
 * This adapter enables {@link JedisConnection} to use the unified API while maintaining
 * a single dedicated connection. Unlike pooled {@link UnifiedJedis} implementations,
 * transactions and pipelines created by this adapter do not close the underlying connection.
 *
 * @author Tihomir Mateev
 * @since 4.1
 * @see UnifiedJedis
 * @see JedisConnection
 */
public class UnifiedJedisAdapter extends UnifiedJedis {

	private final Jedis jedis;

	/**
	 * Creates a new adapter wrapping the given {@link Jedis} instance.
	 *
	 * @param jedis the Jedis instance to wrap
	 */
	public UnifiedJedisAdapter(Jedis jedis) {
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
		return new Transaction(jedis);
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

	@Override
	public ScanResult<byte[]> scan(byte[] cursor, ScanParams params) {
		return jedis.scan(cursor, params);
	}

	@Override
	public ScanResult<byte[]> scan(byte[] cursor, ScanParams params, byte[] type) {
		return jedis.scan(cursor, params, type);
	}

	@Override
	public ScanResult<byte[]> sscan(byte[] key, byte[] cursor, ScanParams params) {
		return jedis.sscan(key, cursor, params);
	}

	@Override
	public ScanResult<Tuple> zscan(byte[] key, byte[] cursor, ScanParams params) {
		return jedis.zscan(key, cursor, params);
	}

	@Override
	public ScanResult<Entry<byte[], byte[]>> hscan(byte[] key, byte[] cursor, ScanParams params) {
		return jedis.hscan(key, cursor, params);
	}
}
