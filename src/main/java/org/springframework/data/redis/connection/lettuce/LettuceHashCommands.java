/*
 * Copyright 2017-2024 the original author or authors.
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
package org.springframework.data.redis.connection.lettuce;

import io.lettuce.core.KeyValue;
import io.lettuce.core.MapScanCursor;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.api.async.RedisHashAsyncCommands;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.RedisHashCommands;
import org.springframework.data.redis.connection.convert.Converters;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.Cursor.CursorId;
import org.springframework.data.redis.core.KeyBoundCursor;
import org.springframework.data.redis.core.ScanIteration;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Tihomir Mateev
 * @since 2.0
 */
class LettuceHashCommands implements RedisHashCommands {

	private final LettuceConnection connection;

	LettuceHashCommands(LettuceConnection connection) {
		this.connection = connection;
	}

	@Override
	public Boolean hSet(byte[] key, byte[] field, byte[] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(field, "Field must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke().just(RedisHashAsyncCommands::hset, key, field, value);
	}

	@Override
	public Boolean hSetNX(byte[] key, byte[] field, byte[] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(field, "Field must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke().just(RedisHashAsyncCommands::hsetnx, key, field, value);
	}

	@Override
	public Long hDel(byte[] key, byte[]... fields) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(fields, "Fields must not be null");

		return connection.invoke().just(RedisHashAsyncCommands::hdel, key, fields);
	}

	@Override
	public Boolean hExists(byte[] key, byte[] field) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(field, "Fields must not be null");

		return connection.invoke().just(RedisHashAsyncCommands::hexists, key, field);
	}

	@Override
	public byte[] hGet(byte[] key, byte[] field) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(field, "Field must not be null");

		return connection.invoke().just(RedisHashAsyncCommands::hget, key, field);
	}

	@Override
	public Map<byte[], byte[]> hGetAll(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(RedisHashAsyncCommands::hgetall, key);
	}

	@Nullable
	@Override
	public byte[] hRandField(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(RedisHashAsyncCommands::hrandfield, key);
	}

	@Nullable
	@Override
	public Entry<byte[], byte[]> hRandFieldWithValues(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().from(RedisHashAsyncCommands::hrandfieldWithvalues, key)
				.get(LettuceHashCommands::toEntry);
	}

	@Nullable
	@Override
	public List<byte[]> hRandField(byte[] key, long count) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(RedisHashAsyncCommands::hrandfield, key, count);
	}

	@Nullable
	@Override
	public List<Entry<byte[], byte[]>> hRandFieldWithValues(byte[] key, long count) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().fromMany(RedisHashAsyncCommands::hrandfieldWithvalues, key, count)
				.toList(LettuceHashCommands::toEntry);
	}

	@Override
	public Long hIncrBy(byte[] key, byte[] field, long delta) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(field, "Field must not be null");

		return connection.invoke().just(RedisHashAsyncCommands::hincrby, key, field, delta);
	}

	@Override
	public Double hIncrBy(byte[] key, byte[] field, double delta) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(field, "Field must not be null");

		return connection.invoke().just(RedisHashAsyncCommands::hincrbyfloat, key, field, delta);
	}

	@Override
	public Set<byte[]> hKeys(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().fromMany(RedisHashAsyncCommands::hkeys, key).toSet();
	}

	@Override
	public Long hLen(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(RedisHashAsyncCommands::hlen, key);
	}

	@Override
	public List<byte[]> hMGet(byte[] key, byte[]... fields) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(fields, "Fields must not be null");

		return connection.invoke().fromMany(RedisHashAsyncCommands::hmget, key, fields)
				.toList(source -> source.getValueOrElse(null));
	}

	@Override
	public void hMSet(byte[] key, Map<byte[], byte[]> hashes) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(hashes, "Hashes must not be null");

		connection.invokeStatus().just(RedisHashAsyncCommands::hmset, key, hashes);
	}

	@Override
	public List<byte[]> hVals(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(RedisHashAsyncCommands::hvals, key);
	}

	@Override
	public Cursor<Entry<byte[], byte[]>> hScan(byte[] key, ScanOptions options) {
		return hScan(key, CursorId.initial(), options);
	}

	@Override
	public List<Long> hExpire(byte[] key, long seconds, byte[]... fields) {
		return connection.invoke().fromMany(RedisHashAsyncCommands::hexpire, key, seconds, fields).toList();
	}

	@Override
	public List<Long> hpExpire(byte[] key, long millis, byte[]... fields) {
		return connection.invoke().fromMany(RedisHashAsyncCommands::hpexpire, key, millis, fields).toList();
	}

	@Override
	public List<Long> hExpireAt(byte[] key, long unixTime, byte[]... fields) {
		return connection.invoke().fromMany(RedisHashAsyncCommands::hexpireat, key, unixTime, fields).toList();
	}

	@Override
	public List<Long> hpExpireAt(byte[] key, long unixTimeInMillis, byte[]... fields) {
		return connection.invoke().fromMany(RedisHashAsyncCommands::hpexpireat, key, unixTimeInMillis, fields).toList();
	}

	@Override
	public List<Long> hPersist(byte[] key, byte[]... fields) {
		return connection.invoke().fromMany(RedisHashAsyncCommands::hpersist, key, fields).toList();
	}

	@Override
	public List<Long> hTtl(byte[] key, byte[]... fields) {
		return connection.invoke().fromMany(RedisHashAsyncCommands::httl, key, fields).toList();
	}

	@Override
	public List<Long> hTtl(byte[] key, TimeUnit timeUnit, byte[]... fields) {
		return connection.invoke().fromMany(RedisHashAsyncCommands::httl, key, fields)
				.toList(Converters.secondsToTimeUnit(timeUnit));
	}

	/**
	 * @param key
	 * @param cursorId
	 * @param options
	 * @return
	 * @since 1.4
	 */
	public Cursor<Entry<byte[], byte[]>> hScan(byte[] key, CursorId cursorId, ScanOptions options) {

		Assert.notNull(key, "Key must not be null");

		return new KeyBoundCursor<Entry<byte[], byte[]>>(key, cursorId, options) {

			@Override
			protected ScanIteration<Entry<byte[], byte[]>> doScan(byte[] key, CursorId cursorId, ScanOptions options) {

				if (connection.isQueueing() || connection.isPipelined()) {
					throw new InvalidDataAccessApiUsageException("'HSCAN' cannot be called in pipeline / transaction mode");
				}

				io.lettuce.core.ScanCursor scanCursor = connection.getScanCursor(cursorId);
				ScanArgs scanArgs = LettuceConverters.toScanArgs(options);

				MapScanCursor<byte[], byte[]> mapScanCursor = connection.invoke().just(RedisHashAsyncCommands::hscan, key,
						scanCursor, scanArgs);
				String nextCursorId = mapScanCursor.getCursor();

				Map<byte[], byte[]> values = mapScanCursor.getMap();
				return new ScanIteration<>(CursorId.of(nextCursorId), values.entrySet());
			}

			@Override
			protected void doClose() {
				LettuceHashCommands.this.connection.close();
			}

		}.open();
	}

	@Nullable
	@Override
	public Long hStrLen(byte[] key, byte[] field) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(field, "Field must not be null");

		return connection.invoke().just(RedisHashAsyncCommands::hstrlen, key, field);
	}

	@Nullable
	private static Entry<byte[], byte[]> toEntry(KeyValue<byte[], byte[]> value) {
		return value.hasValue() ? Converters.entryOf(value.getKey(), value.getValue()) : null;
	}

}
