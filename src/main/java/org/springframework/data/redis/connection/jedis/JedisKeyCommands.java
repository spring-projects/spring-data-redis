/*
 * Copyright 2017-present the original author or authors.
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

import redis.clients.jedis.args.ExpiryOption;
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

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.ExpirationOptions;
import org.springframework.data.redis.connection.RedisKeyCommands;
import org.springframework.data.redis.connection.SortParameters;
import org.springframework.data.redis.connection.ValueEncoding;
import org.springframework.data.redis.connection.ValueEncoding.RedisValueEncoding;
import org.springframework.data.redis.connection.convert.Converters;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.Cursor.CursorId;
import org.springframework.data.redis.core.KeyScanOptions;
import org.springframework.data.redis.core.ScanCursor;
import org.springframework.data.redis.core.ScanIteration;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author ihaohong
 * @since 2.0
 */
@NullUnmarked
class JedisKeyCommands implements RedisKeyCommands {

	private final JedisConnection connection;

	JedisKeyCommands(@NonNull JedisConnection connection) {
		this.connection = connection;
	}

	@Override
	public Boolean exists(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(JedisBinaryCommands::exists, PipelineBinaryCommands::exists, key);
	}

	@Override
	public Long exists(byte @NonNull [] @NonNull... keys) {

		Assert.notNull(keys, "Keys must not be null");
		Assert.noNullElements(keys, "Keys must not contain null elements");

		return connection.invoke().just(JedisBinaryCommands::exists, PipelineBinaryCommands::exists, keys);
	}

	@Override
	public Long del(byte @NonNull [] @NonNull... keys) {

		Assert.notNull(keys, "Keys must not be null");
		Assert.noNullElements(keys, "Keys must not contain null elements");

		return connection.invoke().just(JedisBinaryCommands::del, PipelineBinaryCommands::del, keys);
	}

	@Override
	public Boolean copy(byte @NonNull [] sourceKey, byte @NonNull [] targetKey, boolean replace) {

		Assert.notNull(sourceKey, "source key must not be null");
		Assert.notNull(targetKey, "target key must not be null");

		return connection.invoke().just(JedisBinaryCommands::copy, PipelineBinaryCommands::copy, sourceKey, targetKey,
				replace);
	}

	@Override
	public Long unlink(byte @NonNull [] @NonNull... keys) {

		Assert.notNull(keys, "Keys must not be null");

		return connection.invoke().just(JedisBinaryCommands::unlink, PipelineBinaryCommands::unlink, keys);
	}

	@Override
	public DataType type(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().from(JedisBinaryCommands::type, PipelineBinaryCommands::type, key)
				.get(JedisConverters.stringToDataType());
	}

	@Override
	public Long touch(byte @NonNull [] @NonNull... keys) {

		Assert.notNull(keys, "Keys must not be null");

		return connection.invoke().just(JedisBinaryCommands::touch, PipelineBinaryCommands::touch, keys);
	}

	@Override
	public Set<byte @NonNull []> keys(byte @NonNull [] pattern) {

		Assert.notNull(pattern, "Pattern must not be null");

		return connection.invoke().just(JedisBinaryCommands::keys, PipelineBinaryCommands::keys, pattern);
	}

	@Override
	public Cursor<byte @NonNull []> scan(@NonNull ScanOptions options) {
		return scan(CursorId.initial(), options != null ? options : ScanOptions.NONE);
	}

	/**
	 * @since 1.4
	 * @param cursorId
	 * @param options
	 * @return
	 */
	public Cursor<byte @NonNull []> scan(@NonNull CursorId cursorId, @NonNull ScanOptions options) {

		return new ScanCursor<byte[]>(cursorId, options) {

			@Override
			protected ScanIteration<byte[]> doScan(CursorId cursorId, ScanOptions options) {

				if (isQueueing() || isPipelined()) {
					throw new InvalidDataAccessApiUsageException("'SCAN' cannot be called in pipeline / transaction mode");
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
					result = connection.getJedis().scan(JedisConverters.toBytes(cursorId), params, type);
				} else {
					result = connection.getJedis().scan(JedisConverters.toBytes(cursorId), params);
				}

				return new ScanIteration<>(CursorId.of(result.getCursor()), result.getResult());
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
	public void rename(byte @NonNull [] oldKey, byte @NonNull [] newKey) {

		Assert.notNull(oldKey, "Old key must not be null");
		Assert.notNull(newKey, "New key must not be null");

		connection.invokeStatus().just(JedisBinaryCommands::rename, PipelineBinaryCommands::rename, oldKey, newKey);
	}

	@Override
	public Boolean renameNX(byte @NonNull [] sourceKey, byte @NonNull [] targetKey) {

		Assert.notNull(sourceKey, "Source key must not be null");
		Assert.notNull(targetKey, "Target key must not be null");

		return connection.invoke()
				.from(JedisBinaryCommands::renamenx, PipelineBinaryCommands::renamenx, sourceKey, targetKey)
				.get(JedisConverters.longToBoolean());
	}

	@Override
	public Boolean expire(byte @NonNull [] key, long seconds, ExpirationOptions.@NonNull Condition condition) {

		Assert.notNull(key, "Key must not be null");

		if (seconds > Integer.MAX_VALUE) {
			return pExpire(key, TimeUnit.SECONDS.toMillis(seconds), condition);
		}

		if (condition == ExpirationOptions.Condition.ALWAYS) {
			return connection.invoke().from(JedisBinaryCommands::expire, PipelineBinaryCommands::expire, key, seconds)
					.get(JedisConverters.longToBoolean());
		}

		ExpiryOption option = ExpiryOption.valueOf(condition.name());
		return connection.invoke().from(JedisBinaryCommands::expire, PipelineBinaryCommands::expire, key, seconds, option)
				.get(JedisConverters.longToBoolean());
	}

	@Override
	public Boolean pExpire(byte @NonNull [] key, long millis, ExpirationOptions.@NonNull Condition condition) {

		Assert.notNull(key, "Key must not be null");

		if (condition == ExpirationOptions.Condition.ALWAYS) {
			return connection.invoke().from(JedisBinaryCommands::pexpire, PipelineBinaryCommands::pexpire, key, millis)
					.get(JedisConverters.longToBoolean());
		}

		ExpiryOption option = ExpiryOption.valueOf(condition.name());
		return connection.invoke().from(JedisBinaryCommands::pexpire, PipelineBinaryCommands::pexpire, key, millis, option)
				.get(JedisConverters.longToBoolean());
	}

	@Override
	public Boolean expireAt(byte @NonNull [] key, long unixTime, ExpirationOptions.@NonNull Condition condition) {

		Assert.notNull(key, "Key must not be null");

		if (condition == ExpirationOptions.Condition.ALWAYS) {
			return connection.invoke().from(JedisBinaryCommands::expireAt, PipelineBinaryCommands::expireAt, key, unixTime)
					.get(JedisConverters.longToBoolean());
		}

		ExpiryOption option = ExpiryOption.valueOf(condition.name());
		return connection.invoke()
				.from(JedisBinaryCommands::expireAt, PipelineBinaryCommands::expireAt, key, unixTime, option)
				.get(JedisConverters.longToBoolean());
	}

	@Override
	public Boolean pExpireAt(byte @NonNull [] key, long unixTimeInMillis,
			ExpirationOptions.@NonNull Condition condition) {

		Assert.notNull(key, "Key must not be null");

		if (condition == ExpirationOptions.Condition.ALWAYS) {
			return connection.invoke()
					.from(JedisBinaryCommands::pexpireAt, PipelineBinaryCommands::pexpireAt, key, unixTimeInMillis)
					.get(JedisConverters.longToBoolean());
		}

		ExpiryOption option = ExpiryOption.valueOf(condition.name());
		return connection.invoke()
				.from(JedisBinaryCommands::pexpireAt, PipelineBinaryCommands::pexpireAt, key, unixTimeInMillis, option)
				.get(JedisConverters.longToBoolean());
	}

	@Override
	public Boolean persist(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().from(JedisBinaryCommands::persist, PipelineBinaryCommands::persist, key)
				.get(JedisConverters.longToBoolean());
	}

	@Override
	public Boolean move(byte @NonNull [] key, int dbIndex) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().from(j -> j.move(key, dbIndex)).get(JedisConverters.longToBoolean());
	}

	@Override
	public Long ttl(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(JedisBinaryCommands::ttl, PipelineBinaryCommands::ttl, key);
	}

	@Override
	public Long ttl(byte @NonNull [] key, @NonNull TimeUnit timeUnit) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().from(JedisBinaryCommands::ttl, PipelineBinaryCommands::ttl, key)
				.get(Converters.secondsToTimeUnit(timeUnit));
	}

	@Override
	public Long pTtl(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(JedisBinaryCommands::pttl, PipelineBinaryCommands::pttl, key);
	}

	@Override
	public Long pTtl(byte @NonNull [] key, @NonNull TimeUnit timeUnit) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().from(JedisBinaryCommands::pttl, PipelineBinaryCommands::pttl, key)
				.get(Converters.millisecondsToTimeUnit(timeUnit));
	}

	@Override
	public List<byte @NonNull []> sort(byte @NonNull [] key, @Nullable SortParameters params) {

		Assert.notNull(key, "Key must not be null");

		SortingParams sortParams = JedisConverters.toSortingParams(params);

		if (sortParams != null) {
			return connection.invoke().just(JedisBinaryCommands::sort, PipelineBinaryCommands::sort, key, sortParams);
		}

		return connection.invoke().just(JedisBinaryCommands::sort, PipelineBinaryCommands::sort, key);
	}

	@Override
	public Long sort(byte @NonNull [] key, @Nullable SortParameters params, byte @NonNull [] storeKey) {

		Assert.notNull(key, "Key must not be null");

		SortingParams sortParams = JedisConverters.toSortingParams(params);

		if (sortParams != null) {
			return connection.invoke().just(JedisBinaryCommands::sort, PipelineBinaryCommands::sort, key, sortParams,
					storeKey);
		}

		return connection.invoke().just(JedisBinaryCommands::sort, PipelineBinaryCommands::sort, key, storeKey);
	}

	@Override
	public byte[] dump(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(JedisBinaryCommands::dump, PipelineBinaryCommands::dump, key);
	}

	@Override
	public void restore(byte @NonNull [] key, long ttlInMillis, byte @NonNull [] serializedValue, boolean replace) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(serializedValue, "Serialized value must not be null");

		if (replace) {

			connection.invokeStatus().just(JedisBinaryCommands::restore, PipelineBinaryCommands::restore, key,
					(int) ttlInMillis, serializedValue, RestoreParams.restoreParams().replace());
			return;
		}

		if (ttlInMillis > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("TtlInMillis must be less than Integer.MAX_VALUE for restore in Jedis");
		}

		connection.invokeStatus().just(JedisBinaryCommands::restore, PipelineBinaryCommands::restore, key,
				(int) ttlInMillis, serializedValue);
	}

	@Override
	public ValueEncoding encodingOf(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().from(JedisBinaryCommands::objectEncoding, PipelineBinaryCommands::objectEncoding, key)
				.getOrElse(JedisConverters::toEncoding, () -> RedisValueEncoding.VACANT);
	}

	@Override
	public Duration idletime(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().from(JedisBinaryCommands::objectIdletime, PipelineBinaryCommands::objectIdletime, key)
				.get(Converters::secondsToDuration);
	}

	@Override
	public Long refcount(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(JedisBinaryCommands::objectRefcount, PipelineBinaryCommands::objectRefcount, key);
	}

	private boolean isPipelined() {
		return connection.isPipelined();
	}

	private boolean isQueueing() {
		return connection.isQueueing();
	}

}
