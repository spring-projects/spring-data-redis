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

import io.lettuce.core.ScanArgs;
import io.lettuce.core.ValueScanCursor;
import io.lettuce.core.api.async.RedisSetAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.RedisSetCommands;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.Cursor.CursorId;
import org.springframework.data.redis.core.KeyBoundCursor;
import org.springframework.data.redis.core.ScanIteration;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Mingi Lee
 * @since 2.0
 */
@NullUnmarked
class LettuceSetCommands implements RedisSetCommands {

	private final LettuceConnection connection;

	LettuceSetCommands(@NonNull LettuceConnection connection) {
		this.connection = connection;
	}

	@Override
	public Long sAdd(byte @NonNull [] key, byte @NonNull [] @NonNull... values) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(values, "Values must not be null");
		Assert.noNullElements(values, "Values must not contain null elements");

		return connection.invoke().just(RedisSetAsyncCommands::sadd, key, values);
	}

	@Override
	public Long sCard(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(RedisSetAsyncCommands::scard, key);
	}

	@Override
	public Set<byte @NonNull []> sDiff(byte @NonNull [] @NonNull... keys) {

		Assert.notNull(keys, "Keys must not be null");
		Assert.noNullElements(keys, "Keys must not contain null elements");

		return connection.invoke().just(RedisSetAsyncCommands::sdiff, keys);
	}

	@Override
	public Long sDiffStore(byte @NonNull [] destKey, byte @NonNull [] @NonNull... keys) {

		Assert.notNull(destKey, "Destination key must not be null");
		Assert.notNull(keys, "Source keys must not be null");
		Assert.noNullElements(keys, "Source keys must not contain null elements");

		return connection.invoke().just(RedisSetAsyncCommands::sdiffstore, destKey, keys);
	}

	@Override
	public Set<byte @NonNull []> sInter(byte @NonNull [] @NonNull... keys) {

		Assert.notNull(keys, "Keys must not be null");
		Assert.noNullElements(keys, "Keys must not contain null elements");

		return connection.invoke().just(RedisSetAsyncCommands::sinter, keys);
	}

	@Override
	public Long sInterStore(byte @NonNull [] destKey, byte @NonNull [] @NonNull... keys) {

		Assert.notNull(destKey, "Destination key must not be null");
		Assert.notNull(keys, "Source keys must not be null");
		Assert.noNullElements(keys, "Source keys must not contain null elements");

		return connection.invoke().just(RedisSetAsyncCommands::sinterstore, destKey, keys);
	}

	@Override
	public Long sInterCard(byte @NonNull [] @NonNull... keys) {

		Assert.notNull(keys, "Keys must not be null");
		Assert.noNullElements(keys, "Keys must not contain null elements");

		return connection.invoke().just(RedisSetAsyncCommands::sintercard, keys);
	}

	@Override
	public Boolean sIsMember(byte @NonNull [] key, byte @NonNull [] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke().just(RedisSetAsyncCommands::sismember, key, value);
	}

	@Override
	public List<@NonNull Boolean> sMIsMember(byte @NonNull [] key, byte @NonNull [] @NonNull... values) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(values, "Values must not be null");
		Assert.noNullElements(values, "Values must not contain null elements");

		return connection.invoke().just(RedisSetAsyncCommands::smismember, key, values);
	}

	@Override
	public Set<byte @NonNull []> sMembers(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(RedisSetAsyncCommands::smembers, key);
	}

	@Override
	public Boolean sMove(byte @NonNull [] srcKey, byte @NonNull [] destKey, byte @NonNull [] value) {

		Assert.notNull(srcKey, "Source key must not be null");
		Assert.notNull(destKey, "Destination key must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke().just(RedisSetAsyncCommands::smove, srcKey, destKey, value);
	}

	@Override
	public byte[] sPop(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(RedisSetAsyncCommands::spop, key);
	}

	@Override
	public List<byte @NonNull []> sPop(byte @NonNull [] key, long count) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().from(RedisSetAsyncCommands::spop, key, count).get(ArrayList::new);
	}

	@Override
	public byte[] sRandMember(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(RedisSetAsyncCommands::srandmember, key);
	}

	@Override
	public List<byte @NonNull []> sRandMember(byte @NonNull [] key, long count) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(RedisSetAsyncCommands::srandmember, key, count);
	}

	@Override
	public Long sRem(byte @NonNull [] key, byte @NonNull [] @NonNull... values) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(values, "Values must not be null");
		Assert.noNullElements(values, "Values must not contain null elements");

		return connection.invoke().just(RedisSetAsyncCommands::srem, key, values);
	}

	@Override
	public Set<byte @NonNull []> sUnion(byte @NonNull [] @NonNull... keys) {

		Assert.notNull(keys, "Keys must not be null");
		Assert.noNullElements(keys, "Keys must not contain null elements");

		return connection.invoke().just(RedisSetAsyncCommands::sunion, keys);
	}

	@Override
	public Long sUnionStore(byte @NonNull [] destKey, byte @NonNull [] @NonNull... keys) {

		Assert.notNull(destKey, "Destination key must not be null");
		Assert.notNull(keys, "Source keys must not be null");
		Assert.noNullElements(keys, "Source keys must not contain null elements");

		return connection.invoke().just(RedisSetAsyncCommands::sunionstore, destKey, keys);
	}

	@Override
	public Cursor<byte[]> sScan(byte @NonNull [] key, @Nullable ScanOptions options) {
		return sScan(key, CursorId.initial(), options != null ? options : ScanOptions.NONE);
	}

	/**
	 * @param key
	 * @param cursorId
	 * @param options
	 * @return
	 * @since 1.4
	 */
	public Cursor<byte[]> sScan(byte @NonNull [] key, @NonNull CursorId cursorId, @NonNull ScanOptions options) {

		Assert.notNull(key, "Key must not be null");

		return new KeyBoundCursor<byte[]>(key, cursorId, options) {

			@Override
			protected ScanIteration<byte[]> doScan(byte @NonNull [] key, CursorId cursorId, ScanOptions options) {

				if (connection.isQueueing() || connection.isPipelined()) {
					throw new InvalidDataAccessApiUsageException("'SSCAN' cannot be called in pipeline / transaction mode");
				}

				io.lettuce.core.ScanCursor scanCursor = connection.getScanCursor(cursorId);
				ScanArgs scanArgs = LettuceConverters.toScanArgs(options);

				ValueScanCursor<byte[]> valueScanCursor = connection.invoke().just(RedisSetAsyncCommands::sscan, key,
						scanCursor, scanArgs);
				String nextCursorId = valueScanCursor.getCursor();

				List<byte[]> values = connection.failsafeReadScanValues(valueScanCursor.getValues(), null);
				return new ScanIteration<>(CursorId.of(nextCursorId), values);
			}

			protected void doClose() {
				LettuceSetCommands.this.connection.close();
			}

		}.open();
	}

	public RedisClusterCommands<byte[], byte[]> getCommands() {
		return connection.getConnection();
	}

}
