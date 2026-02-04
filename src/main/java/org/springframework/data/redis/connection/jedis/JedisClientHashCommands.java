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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.ExpirationOptions;
import org.springframework.data.redis.connection.RedisHashCommands;
import org.springframework.data.redis.connection.convert.Converters;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.Cursor.CursorId;
import org.springframework.data.redis.core.KeyBoundCursor;
import org.springframework.data.redis.core.ScanIteration;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.util.Assert;

import redis.clients.jedis.args.ExpiryOption;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import static org.springframework.data.redis.connection.ExpirationOptions.Condition.ALWAYS;
import static org.springframework.data.redis.connection.convert.Converters.*;
import static org.springframework.data.redis.connection.jedis.JedisConverters.*;
import static org.springframework.data.redis.core.Cursor.CursorId.of;
import static redis.clients.jedis.args.ExpiryOption.valueOf;

/**
 * {@link RedisHashCommands} implementation for Jedis.
 *
 * @author Tihomir Mateev
 * @since 4.1
 */
@NullUnmarked
class JedisClientHashCommands implements RedisHashCommands {

	private final JedisClientConnection connection;

	JedisClientHashCommands(JedisClientConnection connection) {
		this.connection = connection;
	}

	@Override
	public Boolean hSet(byte @NonNull [] key, byte @NonNull [] field, byte @NonNull [] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(field, "Field must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.execute(client -> client.hset(key, field, value), pipeline -> pipeline.hset(key, field, value),
				longToBoolean());
	}

	@Override
	public Boolean hSetNX(byte @NonNull [] key, byte @NonNull [] field, byte @NonNull [] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(field, "Field must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.execute(client -> client.hsetnx(key, field, value),
				pipeline -> pipeline.hsetnx(key, field, value), longToBoolean());
	}

	@Override
	public Long hDel(byte @NonNull [] key, byte @NonNull [] @NonNull... fields) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(fields, "Fields must not be null");

		return connection.execute(client -> client.hdel(key, fields), pipeline -> pipeline.hdel(key, fields));
	}

	@Override
	public Boolean hExists(byte @NonNull [] key, byte @NonNull [] field) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(field, "Fields must not be null");

		return connection.execute(client -> client.hexists(key, field), pipeline -> pipeline.hexists(key, field));
	}

	@Override
	public byte[] hGet(byte @NonNull [] key, byte @NonNull [] field) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(field, "Field must not be null");

		return connection.execute(client -> client.hget(key, field), pipeline -> pipeline.hget(key, field));
	}

	@Override
	public Map<byte @NonNull [], byte @NonNull []> hGetAll(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.hgetAll(key), pipeline -> pipeline.hgetAll(key));
	}

	@Override
	public byte[] hRandField(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.hrandfield(key), pipeline -> pipeline.hrandfield(key));
	}

	@Nullable
	@Override
	public Entry<byte @NonNull [], byte @NonNull []> hRandFieldWithValues(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.hrandfieldWithValues(key, 1L),
				pipeline -> pipeline.hrandfieldWithValues(key, 1L), result -> !result.isEmpty() ? result.get(0) : null);
	}

	@Nullable
	@Override
	public List<byte @NonNull []> hRandField(byte @NonNull [] key, long count) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.hrandfield(key, count), pipeline -> pipeline.hrandfield(key, count));
	}

	@Nullable
	@Override
	public List<@NonNull Entry<byte @NonNull [], byte @NonNull []>> hRandFieldWithValues(byte @NonNull [] key,
			long count) {

		Assert.notNull(key, "Key must not be null");

		List<Entry<byte[], byte[]>> mapEntryList = connection.execute(client -> client.hrandfieldWithValues(key, count),
				pipeline -> pipeline.hrandfieldWithValues(key, count));

		if (mapEntryList == null) {
			return null;
		}

		List<Entry<byte[], byte[]>> convertedMapEntryList = new ArrayList<>(mapEntryList.size());
		mapEntryList.forEach(entry -> convertedMapEntryList.add(entryOf(entry.getKey(), entry.getValue())));
		return convertedMapEntryList;
	}

	@Override
	public Long hIncrBy(byte @NonNull [] key, byte @NonNull [] field, long delta) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(field, "Field must not be null");

		return connection.execute(client -> client.hincrBy(key, field, delta),
				pipeline -> pipeline.hincrBy(key, field, delta));
	}

	@Override
	public Double hIncrBy(byte @NonNull [] key, byte @NonNull [] field, double delta) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(field, "Field must not be null");

		return connection.execute(client -> client.hincrByFloat(key, field, delta),
				pipeline -> pipeline.hincrByFloat(key, field, delta));
	}

	@Override
	public Set<byte @NonNull []> hKeys(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.hkeys(key), pipeline -> pipeline.hkeys(key));
	}

	@Override
	public Long hLen(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.hlen(key), pipeline -> pipeline.hlen(key));
	}

	@Override
	public List<byte[]> hMGet(byte @NonNull [] key, byte @NonNull [] @NonNull... fields) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(fields, "Fields must not be null");

		return connection.execute(client -> client.hmget(key, fields), pipeline -> pipeline.hmget(key, fields));
	}

	@Override
	public void hMSet(byte @NonNull [] key, @NonNull Map<byte @NonNull [], byte @NonNull []> hashes) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(hashes, "Hashes must not be null");

		connection.executeStatus(client -> client.hmset(key, hashes), pipeline -> pipeline.hmset(key, hashes));
	}

	@Override
	public List<byte @NonNull []> hVals(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.hvals(key), pipeline -> pipeline.hvals(key));
	}

	@Override
	public Cursor<@NonNull Entry<byte @NonNull [], byte @NonNull []>> hScan(byte @NonNull [] key,
			@NonNull ScanOptions options) {
		return hScan(key, CursorId.initial(), options);
	}

	public Cursor<@NonNull Entry<byte @NonNull [], byte @NonNull []>> hScan(byte @NonNull [] key,
			@NonNull CursorId cursorId, @NonNull ScanOptions options) {

		Assert.notNull(key, "Key must not be null");

		return new KeyBoundCursor<Entry<byte[], byte[]>>(key, cursorId, options) {

			@Override
			protected ScanIteration<Entry<byte[], byte[]>> doScan(byte @NonNull [] key, @NonNull CursorId cursorId,
					@NonNull ScanOptions options) {

				if (isQueueing() || isPipelined()) {
					throw new InvalidDataAccessApiUsageException("'HSCAN' cannot be called in pipeline / transaction mode");
				}

				ScanParams params = toScanParams(options);

				ScanResult<Entry<byte[], byte[]>> result = connection.getJedis().hscan(key, toBytes(cursorId), params);
				return new ScanIteration<>(of(result.getCursor()), result.getResult());
			}

			@Override
			protected void doClose() {
				JedisClientHashCommands.this.connection.close();
			}

		}.open();
	}

	@Override
	public List<@NonNull Long> hExpire(byte @NonNull [] key, long seconds, ExpirationOptions.@NonNull Condition condition,
			byte @NonNull [] @NonNull... fields) {

		if (condition == ALWAYS) {
			return connection.execute(client -> client.hexpire(key, seconds, fields),
					pipeline -> pipeline.hexpire(key, seconds, fields));
		}

		ExpiryOption option = valueOf(condition.name());
		return connection.execute(client -> client.hexpire(key, seconds, option, fields),
				pipeline -> pipeline.hexpire(key, seconds, option, fields));
	}

	@Override
	public List<@NonNull Long> hpExpire(byte @NonNull [] key, long millis, ExpirationOptions.@NonNull Condition condition,
			byte @NonNull [] @NonNull... fields) {

		if (condition == ALWAYS) {
			return connection.execute(client -> client.hpexpire(key, millis, fields),
					pipeline -> pipeline.hpexpire(key, millis, fields));
		}

		ExpiryOption option = valueOf(condition.name());
		return connection.execute(client -> client.hpexpire(key, millis, option, fields),
				pipeline -> pipeline.hpexpire(key, millis, option, fields));
	}

	@Override
	public List<@NonNull Long> hExpireAt(byte @NonNull [] key, long unixTime,
			ExpirationOptions.@NonNull Condition condition, byte @NonNull [] @NonNull... fields) {

		if (condition == ALWAYS) {
			return connection.execute(client -> client.hexpireAt(key, unixTime, fields),
					pipeline -> pipeline.hexpireAt(key, unixTime, fields));
		}

		ExpiryOption option = valueOf(condition.name());
		return connection.execute(client -> client.hexpireAt(key, unixTime, option, fields),
				pipeline -> pipeline.hexpireAt(key, unixTime, option, fields));
	}

	@Override
	public List<@NonNull Long> hpExpireAt(byte @NonNull [] key, long unixTimeInMillis,
			ExpirationOptions.@NonNull Condition condition, byte @NonNull [] @NonNull... fields) {

		if (condition == ALWAYS) {
			return connection.execute(client -> client.hpexpireAt(key, unixTimeInMillis, fields),
					pipeline -> pipeline.hpexpireAt(key, unixTimeInMillis, fields));
		}

		ExpiryOption option = valueOf(condition.name());
		return connection.execute(client -> client.hpexpireAt(key, unixTimeInMillis, option, fields),
				pipeline -> pipeline.hpexpireAt(key, unixTimeInMillis, option, fields));
	}

	@Override
	public List<@NonNull Long> hPersist(byte @NonNull [] key, byte @NonNull [] @NonNull... fields) {
		return connection.execute(client -> client.hpersist(key, fields), pipeline -> pipeline.hpersist(key, fields));
	}

	@Override
	public List<@NonNull Long> hTtl(byte @NonNull [] key, byte @NonNull [] @NonNull... fields) {
		return connection.execute(client -> client.httl(key, fields), pipeline -> pipeline.httl(key, fields));
	}

	@Override
	public List<@NonNull Long> hTtl(byte @NonNull [] key, @NonNull TimeUnit timeUnit,
			byte @NonNull [] @NonNull... fields) {
		List<Long> result = connection.execute(client -> client.httl(key, fields), pipeline -> pipeline.httl(key, fields));

		if (result == null) {
			return null;
		}

		List<Long> converted = new ArrayList<>(result.size());
		for (Long value : result) {
			converted.add(value != null ? secondsToTimeUnit(timeUnit).convert(value) : null);
		}
		return converted;
	}

	@Override
	public List<@NonNull Long> hpTtl(byte @NonNull [] key, byte @NonNull [] @NonNull... fields) {
		return connection.execute(client -> client.hpttl(key, fields), pipeline -> pipeline.hpttl(key, fields));
	}

	@Override
	public List<byte[]> hGetDel(byte @NonNull [] key, byte @NonNull [] @NonNull... fields) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(fields, "Fields must not be null");

		return connection.execute(client -> client.hgetdel(key, fields), pipeline -> pipeline.hgetdel(key, fields));
	}

	@Override
	public List<byte[]> hGetEx(byte @NonNull [] key, @Nullable Expiration expiration,
			byte @NonNull [] @NonNull... fields) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(fields, "Fields must not be null");

		return connection.execute(client -> client.hgetex(key, toHGetExParams(expiration), fields),
				pipeline -> pipeline.hgetex(key, toHGetExParams(expiration), fields));
	}

	@Override
	public Boolean hSetEx(byte @NonNull [] key, @NonNull Map<byte[], byte[]> hashes,
			@NonNull HashFieldSetOption condition, @Nullable Expiration expiration) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(hashes, "Hashes must not be null");
		Assert.notNull(condition, "Condition must not be null");

		return connection.execute(client -> client.hsetex(key, toHSetExParams(condition, expiration), hashes),
				pipeline -> pipeline.hsetex(key, toHSetExParams(condition, expiration), hashes), Converters::toBoolean);
	}

	@Nullable
	@Override
	public Long hStrLen(byte @NonNull [] key, byte @NonNull [] field) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(field, "Field must not be null");

		return connection.execute(client -> client.hstrlen(key, field), pipeline -> pipeline.hstrlen(key, field));
	}

	private boolean isPipelined() {
		return connection.isPipelined();
	}

	private boolean isQueueing() {
		return connection.isQueueing();
	}

}
