/*
 * Copyright 2017-2023 the original author or authors.
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

import org.springframework.data.redis.connection.RedisSetCommands;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.KeyBoundCursor;
import org.springframework.data.redis.core.ScanIteration;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
class LettuceSetCommands implements RedisSetCommands {

	private final LettuceConnection connection;

	LettuceSetCommands(LettuceConnection connection) {
		this.connection = connection;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sAdd(byte[], byte[][])
	 */
	@Override
	public Long sAdd(byte[] key, byte[]... values) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(values, "Values must not be null!");
		Assert.noNullElements(values, "Values must not contain null elements!");

		return connection.invoke().just(RedisSetAsyncCommands::sadd, key, values);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sCard(byte[])
	 */
	@Override
	public Long sCard(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(RedisSetAsyncCommands::scard, key);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sDiff(byte[][])
	 */
	@Override
	public Set<byte[]> sDiff(byte[]... keys) {

		Assert.notNull(keys, "Keys must not be null!");
		Assert.noNullElements(keys, "Keys must not contain null elements!");

		return connection.invoke().just(RedisSetAsyncCommands::sdiff, keys);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sDiffStore(byte[], byte[][])
	 */
	@Override
	public Long sDiffStore(byte[] destKey, byte[]... keys) {

		Assert.notNull(destKey, "Destination key must not be null!");
		Assert.notNull(keys, "Source keys must not be null!");
		Assert.noNullElements(keys, "Source keys must not contain null elements!");

		return connection.invoke().just(RedisSetAsyncCommands::sdiffstore, destKey, keys);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sInter(byte[][])
	 */
	@Override
	public Set<byte[]> sInter(byte[]... keys) {

		Assert.notNull(keys, "Keys must not be null!");
		Assert.noNullElements(keys, "Keys must not contain null elements!");

		return connection.invoke().just(RedisSetAsyncCommands::sinter, keys);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sInterStore(byte[], byte[][])
	 */
	@Override
	public Long sInterStore(byte[] destKey, byte[]... keys) {

		Assert.notNull(destKey, "Destination key must not be null!");
		Assert.notNull(keys, "Source keys must not be null!");
		Assert.noNullElements(keys, "Source keys must not contain null elements!");

		return connection.invoke().just(RedisSetAsyncCommands::sinterstore, destKey, keys);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sIsMember(byte[], byte[])
	 */
	@Override
	public Boolean sIsMember(byte[] key, byte[] value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		return connection.invoke().just(RedisSetAsyncCommands::sismember, key, value);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sMIsMember(byte[], byte[]...)
	 */
	@Override
	public List<Boolean> sMIsMember(byte[] key, byte[]... values) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(values, "Values must not be null!");
		Assert.noNullElements(values, "Values must not contain null elements!");

		return connection.invoke().just(RedisSetAsyncCommands::smismember, key, values);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sMembers(byte[])
	 */
	@Override
	public Set<byte[]> sMembers(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(RedisSetAsyncCommands::smembers, key);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sMove(byte[], byte[], byte[])
	 */
	@Override
	public Boolean sMove(byte[] srcKey, byte[] destKey, byte[] value) {

		Assert.notNull(srcKey, "Source key must not be null!");
		Assert.notNull(destKey, "Destination key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		return connection.invoke().just(RedisSetAsyncCommands::smove, srcKey, destKey, value);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sPop(byte[])
	 */
	@Override
	public byte[] sPop(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(RedisSetAsyncCommands::spop, key);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sPop(byte[], long)
	 */
	@Override
	public List<byte[]> sPop(byte[] key, long count) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().from(RedisSetAsyncCommands::spop, key, count).get(ArrayList::new);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sRandMember(byte[])
	 */
	@Override
	public byte[] sRandMember(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(RedisSetAsyncCommands::srandmember, key);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sRandMember(byte[], long)
	 */
	@Override
	public List<byte[]> sRandMember(byte[] key, long count) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(RedisSetAsyncCommands::srandmember, key, count);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sRem(byte[], byte[][])
	 */
	@Override
	public Long sRem(byte[] key, byte[]... values) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(values, "Values must not be null!");
		Assert.noNullElements(values, "Values must not contain null elements!");

		return connection.invoke().just(RedisSetAsyncCommands::srem, key, values);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sUnion(byte[][])
	 */
	@Override
	public Set<byte[]> sUnion(byte[]... keys) {

		Assert.notNull(keys, "Keys must not be null!");
		Assert.noNullElements(keys, "Keys must not contain null elements!");

		return connection.invoke().just(RedisSetAsyncCommands::sunion, keys);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sUnionStore(byte[], byte[][])
	 */
	@Override
	public Long sUnionStore(byte[] destKey, byte[]... keys) {

		Assert.notNull(destKey, "Destination key must not be null!");
		Assert.notNull(keys, "Source keys must not be null!");
		Assert.noNullElements(keys, "Source keys must not contain null elements!");

		return connection.invoke().just(RedisSetAsyncCommands::sunionstore, destKey, keys);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sScan(byte[], org.springframework.data.redis.core.ScanOptions)
	 */
	@Override
	public Cursor<byte[]> sScan(byte[] key, ScanOptions options) {
		return sScan(key, 0, options);
	}

	/**
	 * @since 1.4
	 * @param key
	 * @param cursorId
	 * @param options
	 * @return
	 */
	public Cursor<byte[]> sScan(byte[] key, long cursorId, ScanOptions options) {

		Assert.notNull(key, "Key must not be null!");

		return new KeyBoundCursor<byte[]>(key, cursorId, options) {

			@Override
			protected ScanIteration<byte[]> doScan(byte[] key, long cursorId, ScanOptions options) {

				if (connection.isQueueing() || connection.isPipelined()) {
					throw new UnsupportedOperationException("'SSCAN' cannot be called in pipeline / transaction mode.");
				}

				io.lettuce.core.ScanCursor scanCursor = connection.getScanCursor(cursorId);
				ScanArgs scanArgs = LettuceConverters.toScanArgs(options);

				ValueScanCursor<byte[]> valueScanCursor = connection.invoke().just(RedisSetAsyncCommands::sscan, key,
						scanCursor, scanArgs);
				String nextCursorId = valueScanCursor.getCursor();

				List<byte[]> values = connection.failsafeReadScanValues(valueScanCursor.getValues(), null);
				return new ScanIteration<>(Long.valueOf(nextCursorId), values);
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
