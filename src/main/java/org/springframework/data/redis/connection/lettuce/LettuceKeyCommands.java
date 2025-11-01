/*
 * Copyright 2017-2025 the original author or authors.
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

import io.lettuce.core.CopyArgs;
import io.lettuce.core.ExpireArgs;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.RestoreArgs;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.SortArgs;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.protocol.CommandArgs;

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
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author ihaohong
 * @author Mingyuan Wu
 * @since 2.0
 */
@NullUnmarked
class LettuceKeyCommands implements RedisKeyCommands {

	private final LettuceConnection connection;

	LettuceKeyCommands(@NonNull LettuceConnection connection) {
		this.connection = connection;
	}

	@Override
	public Boolean copy(byte @NonNull [] sourceKey, byte @NonNull [] targetKey, boolean replace) {

		Assert.notNull(sourceKey, "source key must not be null");
		Assert.notNull(targetKey, "target key must not be null");

		return connection.invoke().just(RedisKeyAsyncCommands::copy, sourceKey, targetKey,
				CopyArgs.Builder.replace(replace));
	}

	@Override
	public Boolean exists(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().from(RedisKeyAsyncCommands::exists, key).get(LettuceConverters.longToBooleanConverter());
	}

	@Override
	public Long exists(byte @NonNull [] @NonNull... keys) {

		Assert.notNull(keys, "Keys must not be null");
		Assert.noNullElements(keys, "Keys must not contain null elements");

		return connection.invoke().just(RedisKeyAsyncCommands::exists, keys);
	}

	@Override
	public Long del(byte @NonNull [] @NonNull... keys) {

		Assert.notNull(keys, "Keys must not be null");
		Assert.noNullElements(keys, "Keys must not contain null elements");

		return connection.invoke().just(RedisKeyAsyncCommands::del, keys);
	}

	@Override
	public Long unlink(byte @NonNull [] @NonNull... keys) {

		Assert.notNull(keys, "Keys must not be null");

		return connection.invoke().just(RedisKeyAsyncCommands::unlink, keys);
	}

	@Override
	public DataType type(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().from(RedisKeyAsyncCommands::type, key).get(LettuceConverters.stringToDataType());
	}

	@Override
	public Long touch(byte @NonNull [] @NonNull... keys) {

		Assert.notNull(keys, "Keys must not be null");

		return connection.invoke().just(RedisKeyAsyncCommands::touch, keys);
	}

	@Override
	public Set<byte @NonNull []> keys(byte @NonNull [] pattern) {

		Assert.notNull(pattern, "Pattern must not be null");

		return connection.invoke().fromMany(RedisKeyAsyncCommands::keysLegacy, pattern).toSet();
	}

	/**
	 * @since 1.4
	 * @return
	 */
	public Cursor<byte @NonNull []> scan() {
		return scan(ScanOptions.NONE);
	}

	@Override
	public Cursor<byte @NonNull []> scan(@Nullable ScanOptions options) {
		return doScan(options != null ? options : ScanOptions.NONE);
	}

	/**
	 * @since 1.4
	 * @param options
	 * @return
	 */
	private Cursor<byte[]> doScan(@NonNull ScanOptions options) {

		return new LettuceScanCursor<byte[]>(options) {

			@Override
			protected LettuceScanIteration<byte @NonNull []> doScan(@NonNull ScanCursor cursor,
					@NonNull ScanOptions options) {

				if (connection.isQueueing() || connection.isPipelined()) {
					throw new InvalidDataAccessApiUsageException("'SCAN' cannot be called in pipeline / transaction mode");
				}

				ScanArgs scanArgs = LettuceConverters.toScanArgs(options);
				KeyScanCursor<byte[]> keyScanCursor = connection.invoke().just(RedisKeyAsyncCommands::scan, cursor, scanArgs);
				List<byte[]> keys = keyScanCursor.getKeys();

				return new LettuceScanIteration<>(keyScanCursor, keys);
			}

			@Override
			protected void doClose() {
				LettuceKeyCommands.this.connection.close();
			}

		}.open();
	}

	@Override
	public byte[] randomKey() {
		return connection.invoke().just(RedisKeyAsyncCommands::randomkey);
	}

	@Override
	public void rename(byte @NonNull [] oldKey, byte @NonNull [] newKey) {

		Assert.notNull(oldKey, "Old key must not be null");
		Assert.notNull(newKey, "New key must not be null");

		connection.invokeStatus().just(RedisKeyAsyncCommands::rename, oldKey, newKey);
	}

	@Override
	public Boolean renameNX(byte @NonNull [] sourceKey, byte @NonNull [] targetKey) {

		Assert.notNull(sourceKey, "Source key must not be null");
		Assert.notNull(targetKey, "Target key must not be null");

		return connection.invoke().just(RedisKeyAsyncCommands::renamenx, sourceKey, targetKey);
	}

	@Override
	public Boolean expire(byte @NonNull [] key, long seconds, ExpirationOptions.@NonNull Condition condition) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(RedisKeyAsyncCommands::expire, key, seconds, getExpireArgs(condition));
	}

	@Override
	public Boolean pExpire(byte @NonNull [] key, long millis, ExpirationOptions.@NonNull Condition condition) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(RedisKeyAsyncCommands::pexpire, key, millis, getExpireArgs(condition));
	}

	@Override
	public Boolean expireAt(byte @NonNull [] key, long unixTime, ExpirationOptions.@NonNull Condition condition) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(RedisKeyAsyncCommands::expireat, key, unixTime, getExpireArgs(condition));
	}

	@Override
	public Boolean pExpireAt(byte @NonNull [] key, long unixTimeInMillis,
			ExpirationOptions.@NonNull Condition condition) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(RedisKeyAsyncCommands::pexpireat, key, unixTimeInMillis, getExpireArgs(condition));
	}

	@Override
	public Boolean persist(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(RedisKeyAsyncCommands::persist, key);
	}

	@Override
	public Boolean move(byte @NonNull [] key, int dbIndex) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(RedisKeyAsyncCommands::move, key, dbIndex);
	}

	@Override
	public Long ttl(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(RedisKeyAsyncCommands::ttl, key);
	}

	@Override
	public Long ttl(byte @NonNull [] key, @NonNull TimeUnit timeUnit) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().from(RedisKeyAsyncCommands::ttl, key).get(Converters.secondsToTimeUnit(timeUnit));
	}

	@Override
	public Long pTtl(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(RedisKeyAsyncCommands::pttl, key);
	}

	@Override
	public Long pTtl(byte @NonNull [] key, @NonNull TimeUnit timeUnit) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().from(RedisKeyAsyncCommands::pttl, key).get(Converters.millisecondsToTimeUnit(timeUnit));
	}

	@Override
	public List<byte[]> sort(byte @NonNull [] key, @NonNull SortParameters params) {

		Assert.notNull(key, "Key must not be null");

		SortArgs args = LettuceConverters.toSortArgs(params);

		return connection.invoke().just(RedisKeyAsyncCommands::sort, key, args);
	}

	@Override
	public Long sort(byte @NonNull [] key, @NonNull SortParameters params, byte @NonNull [] sortKey) {

		Assert.notNull(key, "Key must not be null");

		SortArgs args = LettuceConverters.toSortArgs(params);

		return connection.invoke().just(RedisKeyAsyncCommands::sortStore, key, args, sortKey);
	}

	@Override
	public byte[] dump(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(RedisKeyAsyncCommands::dump, key);
	}

	@Override
	public void restore(byte @NonNull [] key, long ttlInMillis, byte @NonNull [] serializedValue, boolean replace) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(serializedValue, "Serialized value must not be null");

		RestoreArgs restoreArgs = RestoreArgs.Builder.ttl(ttlInMillis).replace(replace);

		connection.invokeStatus().just(RedisKeyAsyncCommands::restore, key, serializedValue, restoreArgs);
	}

	@Nullable
	@Override
	public ValueEncoding encodingOf(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().from(RedisKeyAsyncCommands::objectEncoding, key).orElse(ValueEncoding::of,
				RedisValueEncoding.VACANT);
	}

	@Nullable
	@Override
	public Duration idletime(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().from(RedisKeyAsyncCommands::objectIdletime, key).get(Converters::secondsToDuration);
	}

	@Nullable
	@Override
	public Long refcount(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(RedisKeyAsyncCommands::objectRefcount, key);
	}

	private static ExpireArgs getExpireArgs(ExpirationOptions.@NonNull Condition condition) {

		return new ExpireArgs() {
			@Override
			public <K, V> void build(CommandArgs<K, V> args) {

				if (ObjectUtils.nullSafeEquals(condition, ExpirationOptions.Condition.ALWAYS)) {
					return;
				}

				args.add(condition.name());
			}
		};
	}
}
