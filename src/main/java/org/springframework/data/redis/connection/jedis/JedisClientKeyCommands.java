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

import redis.clients.jedis.PipeliningBase;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.args.ExpiryOption;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.params.SortingParams;
import redis.clients.jedis.resps.ScanResult;

import static org.springframework.data.redis.connection.convert.Converters.*;
import static org.springframework.data.redis.connection.convert.Converters.millisecondsToTimeUnit;
import static org.springframework.data.redis.connection.jedis.JedisConverters.toBytes;
import static org.springframework.data.redis.connection.jedis.JedisConverters.toSortingParams;
import static redis.clients.jedis.params.RestoreParams.restoreParams;

/**
 * @author Tihomir Mateev
 * @since 4.1
 */
@NullUnmarked
class JedisClientKeyCommands implements RedisKeyCommands {

	private final JedisClientConnection connection;

	JedisClientKeyCommands(@NonNull JedisClientConnection connection) {
		this.connection = connection;
	}

	@Override
	public Boolean exists(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.exists(key), pipeline -> pipeline.exists(key));
	}

	@Override
	public Long exists(byte @NonNull [] @NonNull... keys) {

		Assert.notNull(keys, "Keys must not be null");
		Assert.noNullElements(keys, "Keys must not contain null elements");

		return connection.execute(client -> client.exists(keys), pipeline -> pipeline.exists(keys));
	}

	@Override
	public Long del(byte @NonNull [] @NonNull... keys) {

		Assert.notNull(keys, "Keys must not be null");
		Assert.noNullElements(keys, "Keys must not contain null elements");

		return connection.execute(client -> client.del(keys), pipeline -> pipeline.del(keys));
	}

	@Override
	public Boolean copy(byte @NonNull [] sourceKey, byte @NonNull [] targetKey, boolean replace) {

		Assert.notNull(sourceKey, "source key must not be null");
		Assert.notNull(targetKey, "target key must not be null");

		return connection.execute(client -> client.copy(sourceKey, targetKey, replace),
				pipeline -> pipeline.copy(sourceKey, targetKey, replace));
	}

	@Override
	public Long unlink(byte @NonNull [] @NonNull... keys) {

		Assert.notNull(keys, "Keys must not be null");

		return connection.execute(client -> client.unlink(keys), pipeline -> pipeline.unlink(keys));
	}

	@Override
	public DataType type(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.type(key), pipeline -> pipeline.type(key),
				JedisConverters.stringToDataType());
	}

	@Override
	public Long touch(byte @NonNull [] @NonNull... keys) {

		Assert.notNull(keys, "Keys must not be null");

		return connection.execute(client -> client.touch(keys), pipeline -> pipeline.touch(keys));
	}

	@Override
	public Set<byte @NonNull []> keys(byte @NonNull [] pattern) {

		Assert.notNull(pattern, "Pattern must not be null");

		return connection.execute(client -> client.keys(pattern), pipeline -> pipeline.keys(pattern));
	}

	@Override
	public Cursor<byte @NonNull []> scan(ScanOptions options) {
		return scan(CursorId.initial(), options);
	}

	/**
	 * @param cursorId the {@link CursorId} to use
	 * @param options the {@link ScanOptions} to use
	 * @return a new {@link Cursor} responsible for hte provided {@link CursorId} and {@link ScanOptions}
	 */
	public Cursor<byte @NonNull []> scan(@NonNull CursorId cursorId, @NonNull ScanOptions options) {

		return new ScanCursor<byte[]>(cursorId, options) {

			@Override
			protected ScanIteration<byte[]> doScan(@NonNull CursorId cursorId, @NonNull ScanOptions options) {

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
					result = connection.getJedis().scan(toBytes(cursorId), params, type);
				} else {
					result = connection.getJedis().scan(toBytes(cursorId), params);
				}

				return new ScanIteration<>(CursorId.of(result.getCursor()), result.getResult());
			}

			protected void doClose() {
				JedisClientKeyCommands.this.connection.close();
			}
		}.open();
	}

	@Override
	public byte[] randomKey() {
		return connection.execute(UnifiedJedis::randomBinaryKey, PipeliningBase::randomBinaryKey);
	}

	@Override
	public void rename(byte @NonNull [] oldKey, byte @NonNull [] newKey) {

		Assert.notNull(oldKey, "Old key must not be null");
		Assert.notNull(newKey, "New key must not be null");

		connection.executeStatus(client -> client.rename(oldKey, newKey), pipeline -> pipeline.rename(oldKey, newKey));
	}

	@Override
	public Boolean renameNX(byte @NonNull [] sourceKey, byte @NonNull [] targetKey) {

		Assert.notNull(sourceKey, "Source key must not be null");
		Assert.notNull(targetKey, "Target key must not be null");

		return connection.execute(client -> client.renamenx(sourceKey, targetKey),
				pipeline -> pipeline.renamenx(sourceKey, targetKey), longToBoolean());
	}

	@Override
	public Boolean expire(byte @NonNull [] key, long seconds, ExpirationOptions.@NonNull Condition condition) {

		Assert.notNull(key, "Key must not be null");

		if (seconds > Integer.MAX_VALUE) {
			return pExpire(key, TimeUnit.SECONDS.toMillis(seconds), condition);
		}

		if (condition == ExpirationOptions.Condition.ALWAYS) {
			return connection.execute(client -> client.expire(key, seconds), pipeline -> pipeline.expire(key, seconds),
					longToBoolean());
		}

		ExpiryOption option = ExpiryOption.valueOf(condition.name());
		return connection.execute(client -> client.expire(key, seconds, option),
				pipeline -> pipeline.expire(key, seconds, option), longToBoolean());
	}

	@Override
	public Boolean pExpire(byte @NonNull [] key, long millis, ExpirationOptions.@NonNull Condition condition) {

		Assert.notNull(key, "Key must not be null");

		if (condition == ExpirationOptions.Condition.ALWAYS) {
			return connection.execute(client -> client.pexpire(key, millis), pipeline -> pipeline.pexpire(key, millis),
					longToBoolean());
		}

		ExpiryOption option = ExpiryOption.valueOf(condition.name());
		return connection.execute(client -> client.pexpire(key, millis, option),
				pipeline -> pipeline.pexpire(key, millis, option), longToBoolean());
	}

	@Override
	public Boolean expireAt(byte @NonNull [] key, long unixTime, ExpirationOptions.@NonNull Condition condition) {

		Assert.notNull(key, "Key must not be null");

		if (condition == ExpirationOptions.Condition.ALWAYS) {
			return connection.execute(client -> client.expireAt(key, unixTime), pipeline -> pipeline.expireAt(key, unixTime),
					longToBoolean());
		}

		ExpiryOption option = ExpiryOption.valueOf(condition.name());
		return connection.execute(client -> client.expireAt(key, unixTime, option),
				pipeline -> pipeline.expireAt(key, unixTime, option), longToBoolean());
	}

	@Override
	public Boolean pExpireAt(byte @NonNull [] key, long unixTimeInMillis,
			ExpirationOptions.@NonNull Condition condition) {

		Assert.notNull(key, "Key must not be null");

		if (condition == ExpirationOptions.Condition.ALWAYS) {
			return connection.execute(client -> client.pexpireAt(key, unixTimeInMillis),
					pipeline -> pipeline.pexpireAt(key, unixTimeInMillis), longToBoolean());
		}

		ExpiryOption option = ExpiryOption.valueOf(condition.name());
		return connection.execute(client -> client.pexpireAt(key, unixTimeInMillis, option),
				pipeline -> pipeline.pexpireAt(key, unixTimeInMillis, option), longToBoolean());
	}

	@Override
	public Boolean persist(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.persist(key), pipeline -> pipeline.persist(key), longToBoolean());
	}

	@Override
	public Boolean move(byte @NonNull [] key, int dbIndex) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute("MOVE", false, result -> toBoolean((Long) result), key, toBytes(String.valueOf(dbIndex)));
	}

	@Override
	public Long ttl(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.ttl(key), pipeline -> pipeline.ttl(key));
	}

	@Override
	public Long ttl(byte @NonNull [] key, @NonNull TimeUnit timeUnit) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.ttl(key), pipeline -> pipeline.ttl(key), secondsToTimeUnit(timeUnit));
	}

	@Override
	public Long pTtl(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.pttl(key), pipeline -> pipeline.pttl(key));
	}

	@Override
	public Long pTtl(byte @NonNull [] key, @NonNull TimeUnit timeUnit) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.pttl(key), pipeline -> pipeline.pttl(key),
				millisecondsToTimeUnit(timeUnit));
	}

	@Override
	public List<byte @NonNull []> sort(byte @NonNull [] key, @Nullable SortParameters params) {

		Assert.notNull(key, "Key must not be null");

		SortingParams sortParams = toSortingParams(params);

		if (sortParams != null) {
			return connection.execute(client -> client.sort(key, sortParams), pipeline -> pipeline.sort(key, sortParams));
		}

		return connection.execute(client -> client.sort(key), pipeline -> pipeline.sort(key));
	}

	@Override
	public Long sort(byte @NonNull [] key, @Nullable SortParameters params, byte @NonNull [] storeKey) {

		Assert.notNull(key, "Key must not be null");

		SortingParams sortParams = toSortingParams(params);

		if (sortParams != null) {
			return connection.execute(client -> client.sort(key, sortParams, storeKey),
					pipeline -> pipeline.sort(key, sortParams, storeKey));
		}

		return connection.execute(client -> client.sort(key, storeKey), pipeline -> pipeline.sort(key, storeKey));
	}

	@Override
	public byte[] dump(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.dump(key), pipeline -> pipeline.dump(key));
	}

	@Override
	public void restore(byte @NonNull [] key, long ttlInMillis, byte @NonNull [] serializedValue, boolean replace) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(serializedValue, "Serialized value must not be null");

		if (replace) {

			connection.executeStatus(
					client -> client.restore(key, (int) ttlInMillis, serializedValue, restoreParams().replace()),
					pipeline -> pipeline.restore(key, (int) ttlInMillis, serializedValue, restoreParams().replace()));
			return;
		}

		if (ttlInMillis > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("TtlInMillis must be less than Integer.MAX_VALUE for restore in Jedis");
		}

		connection.executeStatus(client -> client.restore(key, (int) ttlInMillis, serializedValue),
				pipeline -> pipeline.restore(key, (int) ttlInMillis, serializedValue));
	}

	@Override
	public ValueEncoding encodingOf(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.objectEncoding(key), pipeline -> pipeline.objectEncoding(key),
				JedisConverters::toEncoding, () -> RedisValueEncoding.VACANT);
	}

	@Override
	public Duration idletime(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.objectIdletime(key), pipeline -> pipeline.objectIdletime(key),
				Converters::secondsToDuration);
	}

	@Override
	public Long refcount(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.objectRefcount(key), pipeline -> pipeline.objectRefcount(key));
	}

	private boolean isPipelined() {
		return connection.isPipelined();
	}

	private boolean isQueueing() {
		return connection.isQueueing();
	}

}
