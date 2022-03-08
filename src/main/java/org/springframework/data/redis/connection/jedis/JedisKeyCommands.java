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

import redis.clients.jedis.commands.JedisBinaryCommands;
import redis.clients.jedis.commands.PipelineBinaryCommands;
import redis.clients.jedis.params.RestoreParams;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.params.SortingParams;
import redis.clients.jedis.resps.ScanResult;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.RedisKeyCommands;
import org.springframework.data.redis.connection.SortParameters;
import org.springframework.data.redis.connection.ValueEncoding;
import org.springframework.data.redis.connection.ValueEncoding.RedisValueEncoding;
import org.springframework.data.redis.connection.convert.Converters;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.KeyScanOptions;
import org.springframework.data.redis.core.ScanCursor;
import org.springframework.data.redis.core.ScanIteration;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author ihaohong
 * @since 2.0
 */
class JedisKeyCommands implements RedisKeyCommands {

	private final JedisConnection connection;

	JedisKeyCommands(JedisConnection connection) {
		this.connection = connection;
	}

	@Override
	public Boolean exists(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(JedisBinaryCommands::exists, PipelineBinaryCommands::exists, key);
	}

	@Nullable
	@Override
	public Long exists(byte[]... keys) {

		Assert.notNull(keys, "Keys must not be null!");
		Assert.noNullElements(keys, "Keys must not contain null elements!");

		return connection.invoke().just(JedisBinaryCommands::exists, PipelineBinaryCommands::exists, keys);
	}

	@Override
	public Long del(byte[]... keys) {

		Assert.notNull(keys, "Keys must not be null!");
		Assert.noNullElements(keys, "Keys must not contain null elements!");

		return connection.invoke().just(JedisBinaryCommands::del, PipelineBinaryCommands::del, keys);
	}

	public Boolean copy(byte[] sourceKey, byte[] targetKey, boolean replace) {

		Assert.notNull(sourceKey, "source key must not be null!");
		Assert.notNull(targetKey, "target key must not be null!");

		return connection.invoke().just(JedisBinaryCommands::copy, PipelineBinaryCommands::copy, sourceKey, targetKey,
				replace);
	}

	@Nullable
	@Override
	public Long unlink(byte[]... keys) {

		Assert.notNull(keys, "Keys must not be null!");

		return connection.invoke().just(JedisBinaryCommands::unlink, PipelineBinaryCommands::unlink, keys);
	}

	@Override
	public DataType type(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().from(JedisBinaryCommands::type, PipelineBinaryCommands::type, key)
				.get(JedisConverters.stringToDataType());
	}

	@Nullable
	@Override
	public Long touch(byte[]... keys) {

		Assert.notNull(keys, "Keys must not be null!");

		return connection.invoke().just(JedisBinaryCommands::touch, PipelineBinaryCommands::touch, keys);
	}

	@Override
	public Set<byte[]> keys(byte[] pattern) {

		Assert.notNull(pattern, "Pattern must not be null!");

		return connection.invoke().just(JedisBinaryCommands::keys, PipelineBinaryCommands::keys, pattern);
	}

	@Override
	public Cursor<byte[]> scan(ScanOptions options) {
		return scan(0, options != null ? options : ScanOptions.NONE);
	}

	/**
	 * @since 1.4
	 * @param cursorId
	 * @param options
	 * @return
	 */
	public Cursor<byte[]> scan(long cursorId, ScanOptions options) {

		return new ScanCursor<byte[]>(cursorId, options) {

			@Override
			protected ScanIteration<byte[]> doScan(long cursorId, ScanOptions options) {

				if (isQueueing() || isPipelined()) {
					throw new InvalidDataAccessApiUsageException("'SCAN' cannot be called in pipeline / transaction mode.");
				}

				ScanParams params = JedisConverters.toScanParams(options);

				ScanResult<byte[]> result;
				byte[] type = null;

				if (options instanceof KeyScanOptions) {
					String typeAsString = ((KeyScanOptions) options).getType();

					if (!ObjectUtils.isEmpty(typeAsString)) {
						type = typeAsString.getBytes(StandardCharsets.US_ASCII);
					}
				}

				if (type != null) {
					result = connection.getJedis().scan(Long.toString(cursorId).getBytes(), params, type);
				} else {
					result = connection.getJedis().scan(Long.toString(cursorId).getBytes(), params);
				}

				return new ScanIteration<>(Long.parseLong(result.getCursor()), result.getResult());
			}

			protected void doClose() {
				JedisKeyCommands.this.connection.close();
			}
		}.open();
	}

	@Override
	public byte[] randomKey() {
		return connection.invoke().just(JedisBinaryCommands::randomBinaryKey, PipelineBinaryCommands::randomBinaryKey);
	}

	@Override
	public void rename(byte[] oldKey, byte[] newKey) {

		Assert.notNull(oldKey, "Old key must not be null!");
		Assert.notNull(newKey, "New key must not be null!");

		connection.invokeStatus().just(JedisBinaryCommands::rename, PipelineBinaryCommands::rename, oldKey, newKey);
	}

	@Override
	public Boolean renameNX(byte[] sourceKey, byte[] targetKey) {

		Assert.notNull(sourceKey, "Source key must not be null!");
		Assert.notNull(targetKey, "Target key must not be null!");

		return connection.invoke()
				.from(JedisBinaryCommands::renamenx, PipelineBinaryCommands::renamenx, sourceKey, targetKey)
				.get(JedisConverters.longToBoolean());
	}

	@Override
	public Boolean expire(byte[] key, long seconds) {

		Assert.notNull(key, "Key must not be null!");

		if (seconds > Integer.MAX_VALUE) {
			return pExpire(key, TimeUnit.SECONDS.toMillis(seconds));
		}

		return connection.invoke().from(JedisBinaryCommands::expire, PipelineBinaryCommands::expire, key, seconds)
				.get(JedisConverters.longToBoolean());
	}

	@Override
	public Boolean pExpire(byte[] key, long millis) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().from(JedisBinaryCommands::pexpire, PipelineBinaryCommands::pexpire, key, millis)
				.get(JedisConverters.longToBoolean());
	}

	@Override
	public Boolean expireAt(byte[] key, long unixTime) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().from(JedisBinaryCommands::expireAt, PipelineBinaryCommands::expireAt, key, unixTime)
				.get(JedisConverters.longToBoolean());
	}

	@Override
	public Boolean pExpireAt(byte[] key, long unixTimeInMillis) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke()
				.from(JedisBinaryCommands::pexpireAt, PipelineBinaryCommands::pexpireAt, key, unixTimeInMillis)
				.get(JedisConverters.longToBoolean());
	}

	@Override
	public Boolean persist(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().from(JedisBinaryCommands::persist, PipelineBinaryCommands::persist, key)
				.get(JedisConverters.longToBoolean());
	}

	@Override
	public Boolean move(byte[] key, int dbIndex) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().from(j -> j.move(key, dbIndex)).get(JedisConverters.longToBoolean());
	}

	@Override
	public Long ttl(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(JedisBinaryCommands::ttl, PipelineBinaryCommands::ttl, key);
	}

	@Override
	public Long ttl(byte[] key, TimeUnit timeUnit) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().from(JedisBinaryCommands::ttl, PipelineBinaryCommands::ttl, key)
				.get(Converters.secondsToTimeUnit(timeUnit));
	}

	@Override
	public Long pTtl(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(JedisBinaryCommands::pttl, PipelineBinaryCommands::pttl, key);
	}

	@Override
	public Long pTtl(byte[] key, TimeUnit timeUnit) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().from(JedisBinaryCommands::pttl, PipelineBinaryCommands::pttl, key)
				.get(Converters.millisecondsToTimeUnit(timeUnit));
	}

	@Override
	public List<byte[]> sort(byte[] key, SortParameters params) {

		Assert.notNull(key, "Key must not be null!");

		SortingParams sortParams = JedisConverters.toSortingParams(params);

		if (sortParams != null) {
			return connection.invoke().just(JedisBinaryCommands::sort, PipelineBinaryCommands::sort, key, sortParams);
		}

		return connection.invoke().just(JedisBinaryCommands::sort, PipelineBinaryCommands::sort, key);
	}

	@Override
	public Long sort(byte[] key, @Nullable SortParameters params, byte[] storeKey) {

		Assert.notNull(key, "Key must not be null!");

		SortingParams sortParams = JedisConverters.toSortingParams(params);

		if (sortParams != null) {
			return connection.invoke().just(JedisBinaryCommands::sort, PipelineBinaryCommands::sort, key, sortParams,
					storeKey);
		}

		return connection.invoke().just(JedisBinaryCommands::sort, PipelineBinaryCommands::sort, key, storeKey);
	}

	@Override
	public byte[] dump(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(JedisBinaryCommands::dump, PipelineBinaryCommands::dump, key);
	}

	@Override
	public void restore(byte[] key, long ttlInMillis, byte[] serializedValue, boolean replace) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(serializedValue, "Serialized value must not be null!");

		if (replace) {

			connection.invokeStatus().just(JedisBinaryCommands::restore, PipelineBinaryCommands::restore, key,
					(int) ttlInMillis, serializedValue, RestoreParams.restoreParams().replace());
			return;
		}

		if (ttlInMillis > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("TtlInMillis must be less than Integer.MAX_VALUE for restore in Jedis.");
		}

		connection.invokeStatus().just(JedisBinaryCommands::restore, PipelineBinaryCommands::restore, key,
				(int) ttlInMillis, serializedValue);
	}

	@Nullable
	@Override
	public ValueEncoding encodingOf(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().from(JedisBinaryCommands::objectEncoding, PipelineBinaryCommands::objectEncoding, key)
				.getOrElse(JedisConverters::toEncoding, () -> RedisValueEncoding.VACANT);
	}

	@Nullable
	@Override
	public Duration idletime(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().from(JedisBinaryCommands::objectIdletime, PipelineBinaryCommands::objectIdletime, key)
				.get(Converters::secondsToDuration);
	}

	@Nullable
	@Override
	public Long refcount(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(JedisBinaryCommands::objectRefcount, PipelineBinaryCommands::objectRefcount, key);
	}

	private boolean isPipelined() {
		return connection.isPipelined();
	}

	private boolean isQueueing() {
		return connection.isQueueing();
	}

}
