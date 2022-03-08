/*
 * Copyright 2017-2022 the original author or authors.
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

import redis.clients.jedis.Jedis;
import redis.clients.jedis.commands.PipelineBinaryCommands;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.RedisHashCommands;
import org.springframework.data.redis.connection.convert.Converters;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.KeyBoundCursor;
import org.springframework.data.redis.core.ScanIteration;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
class JedisHashCommands implements RedisHashCommands {

	private final JedisConnection connection;

	JedisHashCommands(JedisConnection connection) {
		this.connection = connection;
	}

	@Override
	public Boolean hSet(byte[] key, byte[] field, byte[] value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(field, "Field must not be null!");
		Assert.notNull(value, "Value must not be null!");

		return connection.invoke().from(Jedis::hset, PipelineBinaryCommands::hset, key, field, value)
				.get(JedisConverters.longToBoolean());
	}

	@Override
	public Boolean hSetNX(byte[] key, byte[] field, byte[] value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(field, "Field must not be null!");
		Assert.notNull(value, "Value must not be null!");

		return connection.invoke().from(Jedis::hsetnx, PipelineBinaryCommands::hsetnx, key, field, value)
				.get(JedisConverters.longToBoolean());
	}

	@Override
	public Long hDel(byte[] key, byte[]... fields) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(fields, "Fields must not be null!");

		return connection.invoke().just(Jedis::hdel, PipelineBinaryCommands::hdel, key, fields);
	}

	@Override
	public Boolean hExists(byte[] key, byte[] field) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(field, "Fields must not be null!");

		return connection.invoke().just(Jedis::hexists, PipelineBinaryCommands::hexists, key, field);
	}

	@Override
	public byte[] hGet(byte[] key, byte[] field) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(field, "Field must not be null!");

		return connection.invoke().just(Jedis::hget, PipelineBinaryCommands::hget, key, field);
	}

	@Override
	public Map<byte[], byte[]> hGetAll(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(Jedis::hgetAll, PipelineBinaryCommands::hgetAll, key);
	}

	@Nullable
	@Override
	public byte[] hRandField(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(Jedis::hrandfield, PipelineBinaryCommands::hrandfield, key);
	}

	@Nullable
	@Override
	public Entry<byte[], byte[]> hRandFieldWithValues(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().from(Jedis::hrandfieldWithValues, PipelineBinaryCommands::hrandfieldWithValues, key, 1L)
				.get(it -> it.isEmpty() ? null : it.entrySet().iterator().next());
	}

	@Nullable
	@Override
	public List<byte[]> hRandField(byte[] key, long count) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(Jedis::hrandfield, PipelineBinaryCommands::hrandfield, key, count);
	}

	@Nullable
	@Override
	public List<Entry<byte[], byte[]>> hRandFieldWithValues(byte[] key, long count) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke()
				.from(Jedis::hrandfieldWithValues, PipelineBinaryCommands::hrandfieldWithValues, key, count).get(it -> {

					List<Entry<byte[], byte[]>> entries = new ArrayList<>(it.size());
					it.forEach((k, v) -> entries.add(Converters.entryOf(k, v)));

					return entries;
				});
	}

	@Override
	public Long hIncrBy(byte[] key, byte[] field, long delta) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(field, "Field must not be null!");

		return connection.invoke().just(Jedis::hincrBy, PipelineBinaryCommands::hincrBy, key, field, delta);
	}

	@Override
	public Double hIncrBy(byte[] key, byte[] field, double delta) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(field, "Field must not be null!");

		return connection.invoke().just(Jedis::hincrByFloat, PipelineBinaryCommands::hincrByFloat, key, field, delta);
	}

	@Override
	public Set<byte[]> hKeys(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(Jedis::hkeys, PipelineBinaryCommands::hkeys, key);
	}

	@Override
	public Long hLen(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(Jedis::hlen, PipelineBinaryCommands::hlen, key);
	}

	@Override
	public List<byte[]> hMGet(byte[] key, byte[]... fields) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(fields, "Fields must not be null!");

		return connection.invoke().just(Jedis::hmget, PipelineBinaryCommands::hmget, key, fields);
	}

	@Override
	public void hMSet(byte[] key, Map<byte[], byte[]> hashes) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(hashes, "Hashes must not be null!");

		connection.invokeStatus().just(Jedis::hmset, PipelineBinaryCommands::hmset, key, hashes);
	}

	@Override
	public List<byte[]> hVals(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(Jedis::hvals, PipelineBinaryCommands::hvals, key);
	}

	@Override
	public Cursor<Entry<byte[], byte[]>> hScan(byte[] key, ScanOptions options) {
		return hScan(key, 0, options);
	}

	/**
	 * @since 1.4
	 * @param key
	 * @param cursorId
	 * @param options
	 * @return
	 */
	public Cursor<Entry<byte[], byte[]>> hScan(byte[] key, long cursorId, ScanOptions options) {

		Assert.notNull(key, "Key must not be null!");

		return new KeyBoundCursor<Entry<byte[], byte[]>>(key, cursorId, options) {

			@Override
			protected ScanIteration<Entry<byte[], byte[]>> doScan(byte[] key, long cursorId, ScanOptions options) {

				if (isQueueing() || isPipelined()) {
					throw new InvalidDataAccessApiUsageException("'HSCAN' cannot be called in pipeline / transaction mode.");
				}

				ScanParams params = JedisConverters.toScanParams(options);

				ScanResult<Entry<byte[], byte[]>> result = connection.getJedis().hscan(key, JedisConverters.toBytes(cursorId),
						params);
				return new ScanIteration<>(Long.valueOf(result.getCursor()), result.getResult());
			}

			@Override
			protected void doClose() {
				JedisHashCommands.this.connection.close();
			};

		}.open();
	}

	@Nullable
	@Override
	public Long hStrLen(byte[] key, byte[] field) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(field, "Field must not be null!");

		return connection.invoke().just(Jedis::hstrlen, PipelineBinaryCommands::hstrlen, key, field);
	}

	private boolean isPipelined() {
		return connection.isPipelined();
	}

	private boolean isQueueing() {
		return connection.isQueueing();
	}

}
