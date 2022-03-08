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

import redis.clients.jedis.args.ListDirection;
import redis.clients.jedis.commands.JedisBinaryCommands;
import redis.clients.jedis.commands.PipelineBinaryCommands;
import redis.clients.jedis.params.LPosParams;

import java.util.Collections;
import java.util.List;

import org.springframework.data.redis.connection.RedisListCommands;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author dengliming
 * @since 2.0
 */
class JedisListCommands implements RedisListCommands {

	private final JedisConnection connection;

	JedisListCommands(JedisConnection connection) {
		this.connection = connection;
	}

	@Override
	public Long rPush(byte[] key, byte[]... values) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(JedisBinaryCommands::rpush, PipelineBinaryCommands::rpush, key, values);
	}

	@Override
	public List<Long> lPos(byte[] key, byte[] element, @Nullable Integer rank, @Nullable Integer count) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(element, "Element must not be null!");

		LPosParams params = new LPosParams();
		if (rank != null) {
			params.rank(rank);
		}

		if (count != null) {
			return connection.invoke().just(JedisBinaryCommands::lpos, PipelineBinaryCommands::lpos, key, element, params,
					count);
		}

		return connection.invoke().from(JedisBinaryCommands::lpos, PipelineBinaryCommands::lpos, key, element, params)
				.getOrElse(Collections::singletonList, Collections::emptyList);
	}

	@Override
	public Long lPush(byte[] key, byte[]... values) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(values, "Values must not be null!");
		Assert.noNullElements(values, "Values must not contain null elements!");

		return connection.invoke().just(JedisBinaryCommands::lpush, PipelineBinaryCommands::lpush, key, values);
	}

	@Override
	public Long rPushX(byte[] key, byte[] value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		return connection.invoke().just(JedisBinaryCommands::rpushx, PipelineBinaryCommands::rpushx, key, value);
	}

	@Override
	public Long lPushX(byte[] key, byte[] value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		return connection.invoke().just(JedisBinaryCommands::lpushx, PipelineBinaryCommands::lpushx, key, value);
	}

	@Override
	public Long lLen(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(JedisBinaryCommands::llen, PipelineBinaryCommands::llen, key);
	}

	@Override
	public List<byte[]> lRange(byte[] key, long start, long end) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(JedisBinaryCommands::lrange, PipelineBinaryCommands::lrange, key, start, end);
	}

	@Override
	public void lTrim(byte[] key, long start, long end) {

		Assert.notNull(key, "Key must not be null!");

		connection.invokeStatus().just(JedisBinaryCommands::ltrim, PipelineBinaryCommands::ltrim, key, start, end);
	}

	@Override
	public byte[] lIndex(byte[] key, long index) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(JedisBinaryCommands::lindex, PipelineBinaryCommands::lindex, key, index);
	}

	@Override
	public Long lInsert(byte[] key, Position where, byte[] pivot, byte[] value) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(JedisBinaryCommands::linsert, PipelineBinaryCommands::linsert, key,
				JedisConverters.toListPosition(where), pivot, value);
	}

	@Override
	public byte[] lMove(byte[] sourceKey, byte[] destinationKey, Direction from, Direction to) {

		Assert.notNull(sourceKey, "Source key must not be null!");
		Assert.notNull(destinationKey, "Destination key must not be null!");
		Assert.notNull(from, "From direction must not be null!");
		Assert.notNull(to, "To direction must not be null!");

		return connection.invoke().just(JedisBinaryCommands::lmove, PipelineBinaryCommands::lmove, sourceKey,
				destinationKey,
				ListDirection.valueOf(from.name()), ListDirection.valueOf(to.name()));
	}

	@Override
	public byte[] bLMove(byte[] sourceKey, byte[] destinationKey, Direction from, Direction to, double timeout) {

		Assert.notNull(sourceKey, "Source key must not be null!");
		Assert.notNull(destinationKey, "Destination key must not be null!");
		Assert.notNull(from, "From direction must not be null!");
		Assert.notNull(to, "To direction must not be null!");

		return connection.invoke().just(JedisBinaryCommands::blmove, PipelineBinaryCommands::blmove, sourceKey,
				destinationKey,
				ListDirection.valueOf(from.name()), ListDirection.valueOf(to.name()), timeout);
	}

	@Override
	public void lSet(byte[] key, long index, byte[] value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		connection.invokeStatus().just(JedisBinaryCommands::lset, PipelineBinaryCommands::lset, key, index, value);
	}

	@Override
	public Long lRem(byte[] key, long count, byte[] value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		return connection.invoke().just(JedisBinaryCommands::lrem, PipelineBinaryCommands::lrem, key, count, value);
	}

	@Override
	public byte[] lPop(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(JedisBinaryCommands::lpop, PipelineBinaryCommands::lpop, key);
	}

	@Override
	public List<byte[]> lPop(byte[] key, long count) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(JedisBinaryCommands::lpop, PipelineBinaryCommands::lpop, key, (int) count);
	}

	@Override
	public byte[] rPop(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(JedisBinaryCommands::rpop, PipelineBinaryCommands::rpop, key);
	}

	@Override
	public List<byte[]> rPop(byte[] key, long count) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(JedisBinaryCommands::rpop, PipelineBinaryCommands::rpop, key, (int) count);
	}

	@Override
	public List<byte[]> bLPop(int timeout, byte[]... keys) {

		Assert.notNull(keys, "Key must not be null!");
		Assert.noNullElements(keys, "Keys must not contain null elements!");

		return connection.invoke().just(j -> j.blpop(timeout, keys), j -> j.blpop(timeout, keys));
	}

	@Override
	public List<byte[]> bRPop(int timeout, byte[]... keys) {

		Assert.notNull(keys, "Key must not be null!");
		Assert.noNullElements(keys, "Keys must not contain null elements!");

		return connection.invoke().just(j -> j.brpop(timeout, keys), j -> j.brpop(timeout, keys));
	}

	@Override
	public byte[] rPopLPush(byte[] srcKey, byte[] dstKey) {

		Assert.notNull(srcKey, "Source key must not be null!");
		Assert.notNull(dstKey, "Destination key must not be null!");

		return connection.invoke().just(JedisBinaryCommands::rpoplpush, PipelineBinaryCommands::rpoplpush, srcKey, dstKey);
	}

	@Override
	public byte[] bRPopLPush(int timeout, byte[] srcKey, byte[] dstKey) {

		Assert.notNull(srcKey, "Source key must not be null!");
		Assert.notNull(dstKey, "Destination key must not be null!");

		return connection.invoke().just(JedisBinaryCommands::brpoplpush, PipelineBinaryCommands::brpoplpush, srcKey, dstKey,
				timeout);
	}

}
