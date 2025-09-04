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
package org.springframework.data.redis.connection.jedis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.commands.PipelineBinaryCommands;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
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
class JedisSetCommands implements RedisSetCommands {

	private final JedisConnection connection;

	JedisSetCommands(@NonNull JedisConnection connection) {
		this.connection = connection;
	}

	@Override
	public Long sAdd(byte @NonNull [] key, byte @NonNull []... values) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(values, "Values must not be null");
		Assert.noNullElements(values, "Values must not contain null elements");

		return connection.invoke().just(Jedis::sadd, PipelineBinaryCommands::sadd, key, values);
	}

	@Override
	public Long sCard(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(Jedis::scard, PipelineBinaryCommands::scard, key);
	}

	@Override
	public Set<byte @NonNull []> sDiff(byte @NonNull [] @NonNull... keys) {

		Assert.notNull(keys, "Keys must not be null");
		Assert.noNullElements(keys, "Keys must not contain null elements");

		return connection.invoke().just(Jedis::sdiff, PipelineBinaryCommands::sdiff, keys);
	}

	@Override
	public Long sDiffStore(byte @NonNull [] destKey, byte @NonNull [] @NonNull... keys) {

		Assert.notNull(destKey, "Destination key must not be null");
		Assert.notNull(keys, "Source keys must not be null");
		Assert.noNullElements(keys, "Source keys must not contain null elements");

		return connection.invoke().just(Jedis::sdiffstore, PipelineBinaryCommands::sdiffstore, destKey, keys);
	}

	@Override
	public Set<byte @NonNull []> sInter(byte @NonNull [] @NonNull... keys) {

		Assert.notNull(keys, "Keys must not be null");
		Assert.noNullElements(keys, "Keys must not contain null elements");

		return connection.invoke().just(Jedis::sinter, PipelineBinaryCommands::sinter, keys);
	}

	@Override
	public Long sInterStore(byte @NonNull [] destKey, byte @NonNull [] @NonNull... keys) {

		Assert.notNull(destKey, "Destination key must not be null");
		Assert.notNull(keys, "Source keys must not be null");
		Assert.noNullElements(keys, "Source keys must not contain null elements");

		return connection.invoke().just(Jedis::sinterstore, PipelineBinaryCommands::sinterstore, destKey, keys);
	}

	@Override
	public Long sInterCard(byte @NonNull [] @NonNull... keys) {

		Assert.notNull(keys, "Keys must not be null");
		Assert.noNullElements(keys, "Keys must not contain null elements");

		return connection.invoke().just(Jedis::sintercard, PipelineBinaryCommands::sintercard, keys);
	}

	@Override
	public Boolean sIsMember(byte @NonNull [] key, byte @NonNull [] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke().just(Jedis::sismember, PipelineBinaryCommands::sismember, key, value);
	}

	@Override
	public List<@NonNull Boolean> sMIsMember(byte @NonNull [] key, byte @NonNull [] @NonNull... values) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(values, "Values must not be null");
		Assert.noNullElements(values, "Values must not contain null elements");

		return connection.invoke().just(Jedis::smismember, PipelineBinaryCommands::smismember, key, values);
	}

	@Override
	public Set<byte @NonNull []> sMembers(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(Jedis::smembers, PipelineBinaryCommands::smembers, key);
	}

	@Override
	public Boolean sMove(byte @NonNull [] srcKey, byte @NonNull [] destKey, byte @NonNull [] value) {

		Assert.notNull(srcKey, "Source key must not be null");
		Assert.notNull(destKey, "Destination key must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke().from(Jedis::smove, PipelineBinaryCommands::smove, srcKey, destKey, value)
				.get(JedisConverters::toBoolean);
	}

	@Override
	public byte[] sPop(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(Jedis::spop, PipelineBinaryCommands::spop, key);
	}

	@Override
	public List<byte @NonNull []> sPop(byte @NonNull [] key, long count) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().from(Jedis::spop, PipelineBinaryCommands::spop, key, count).get(ArrayList::new);
	}

	@Override
	public byte[] sRandMember(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(Jedis::srandmember, PipelineBinaryCommands::srandmember, key);
	}

	@Override
	public List<byte @NonNull []> sRandMember(byte @NonNull [] key, long count) {

		Assert.notNull(key, "Key must not be null");

		if (count > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Count must be less than Integer.MAX_VALUE for sRandMember in Jedis");
		}

		return connection.invoke().just(Jedis::srandmember, PipelineBinaryCommands::srandmember, key, (int) count);
	}

	@Override
	public Long sRem(byte @NonNull [] key, byte @NonNull [] @NonNull... values) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(values, "Values must not be null");
		Assert.noNullElements(values, "Values must not contain null elements");

		return connection.invoke().just(Jedis::srem, PipelineBinaryCommands::srem, key, values);
	}

	@Override
	public Set<byte[]> sUnion(byte @NonNull [] @NonNull... keys) {

		Assert.notNull(keys, "Keys must not be null");
		Assert.noNullElements(keys, "Keys must not contain null elements");

		return connection.invoke().just(Jedis::sunion, PipelineBinaryCommands::sunion, keys);
	}

	@Override
	public Long sUnionStore(byte @NonNull [] destKey, byte @NonNull [] @NonNull... keys) {

		Assert.notNull(destKey, "Destination key must not be null");
		Assert.notNull(keys, "Source keys must not be null");
		Assert.noNullElements(keys, "Source keys must not contain null elements");

		return connection.invoke().just(Jedis::sunionstore, PipelineBinaryCommands::sunionstore, destKey, keys);
	}

	@Override
	public Cursor<byte[]> sScan(byte @NonNull [] key, @NonNull ScanOptions options) {
		return sScan(key, CursorId.initial(), options);
	}

	/**
	 * @param key
	 * @param cursorId
	 * @param options
	 * @return
	 * @since 3.2.1
	 */
	public Cursor<byte @NonNull []> sScan(byte @NonNull [] key, @NonNull CursorId cursorId,
			@NonNull ScanOptions options) {

		Assert.notNull(key, "Key must not be null");

		return new KeyBoundCursor<byte[]>(key, cursorId, options) {

			@Override
			protected ScanIteration<byte[]> doScan(byte @NonNull [] key, @NonNull CursorId cursorId,
					@NonNull ScanOptions options) {

				if (isQueueing() || isPipelined()) {
					throw new InvalidDataAccessApiUsageException("'SSCAN' cannot be called in pipeline / transaction mode");
				}

				ScanParams params = JedisConverters.toScanParams(options);

				ScanResult<byte[]> result = connection.getJedis().sscan(key, JedisConverters.toBytes(cursorId), params);
				return new ScanIteration<>(CursorId.of(result.getCursor()), result.getResult());
			}

			protected void doClose() {
				JedisSetCommands.this.connection.close();
			};
		}.open();
	}

	private boolean isPipelined() {
		return connection.isPipelined();
	}

	private boolean isQueueing() {
		return connection.isQueueing();
	}

}
