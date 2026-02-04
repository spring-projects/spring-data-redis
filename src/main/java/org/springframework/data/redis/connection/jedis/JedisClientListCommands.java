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

import java.util.Collections;
import java.util.List;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;
import org.springframework.data.redis.connection.RedisListCommands;
import org.springframework.util.Assert;

import redis.clients.jedis.params.LPosParams;

import static org.springframework.data.redis.connection.jedis.JedisConverters.toListPosition;
import static redis.clients.jedis.args.ListDirection.valueOf;

/**
 * @author Tihomir Mateev
 * @since 4.1
 */
@NullUnmarked
class JedisClientListCommands implements RedisListCommands {

	private final JedisClientConnection connection;

	JedisClientListCommands(@NonNull JedisClientConnection connection) {
		this.connection = connection;
	}

	@Override
	public Long rPush(byte @NonNull [] key, byte @NonNull [] @NonNull... values) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.rpush(key, values), pipeline -> pipeline.rpush(key, values));
	}

	@Override
	public List<Long> lPos(byte @NonNull [] key, byte @NonNull [] element, @Nullable Integer rank,
			@Nullable Integer count) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(element, "Element must not be null");

		LPosParams params = new LPosParams();
		if (rank != null) {
			params.rank(rank);
		}

		if (count != null) {
			return connection.execute(client -> client.lpos(key, element, params, count),
					pipeline -> pipeline.lpos(key, element, params, count), result -> result, Collections::emptyList);
		}

		return connection.execute(client -> client.lpos(key, element, params),
				pipeline -> pipeline.lpos(key, element, params), Collections::singletonList, Collections::emptyList);
	}

	@Override
	public Long lPush(byte @NonNull [] key, byte @NonNull [] @NonNull... values) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(values, "Values must not be null");
		Assert.noNullElements(values, "Values must not contain null elements");

		return connection.execute(client -> client.lpush(key, values), pipeline -> pipeline.lpush(key, values));
	}

	@Override
	public Long rPushX(byte @NonNull [] key, byte @NonNull [] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.execute(client -> client.rpushx(key, value), pipeline -> pipeline.rpushx(key, value));
	}

	@Override
	public Long lPushX(byte @NonNull [] key, byte @NonNull [] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.execute(client -> client.lpushx(key, value), pipeline -> pipeline.lpushx(key, value));
	}

	@Override
	public Long lLen(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.llen(key), pipeline -> pipeline.llen(key));
	}

	@Override
	public List<byte @NonNull []> lRange(byte @NonNull [] key, long start, long end) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.lrange(key, start, end), pipeline -> pipeline.lrange(key, start, end));
	}

	@Override
	public void lTrim(byte @NonNull [] key, long start, long end) {

		Assert.notNull(key, "Key must not be null");

		connection.executeStatus(client -> client.ltrim(key, start, end), pipeline -> pipeline.ltrim(key, start, end));
	}

	@Override
	public byte[] lIndex(byte @NonNull [] key, long index) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.lindex(key, index), pipeline -> pipeline.lindex(key, index));
	}

	@Override
	public Long lInsert(byte @NonNull [] key, @NonNull Position where, byte @NonNull [] pivot, byte @NonNull [] value) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.linsert(key, toListPosition(where), pivot, value),
				pipeline -> pipeline.linsert(key, toListPosition(where), pivot, value));
	}

	@Override
	public byte[] lMove(byte @NonNull [] sourceKey, byte @NonNull [] destinationKey, @NonNull Direction from,
			@NonNull Direction to) {

		Assert.notNull(sourceKey, "Source key must not be null");
		Assert.notNull(destinationKey, "Destination key must not be null");
		Assert.notNull(from, "From direction must not be null");
		Assert.notNull(to, "To direction must not be null");

		return connection.execute(
				client -> client.lmove(sourceKey, destinationKey, valueOf(from.name()), valueOf(to.name())),
				pipeline -> pipeline.lmove(sourceKey, destinationKey, valueOf(from.name()), valueOf(to.name())));
	}

	@Override
	public byte[] bLMove(byte @NonNull [] sourceKey, byte @NonNull [] destinationKey, @NonNull Direction from,
			@NonNull Direction to, double timeout) {

		Assert.notNull(sourceKey, "Source key must not be null");
		Assert.notNull(destinationKey, "Destination key must not be null");
		Assert.notNull(from, "From direction must not be null");
		Assert.notNull(to, "To direction must not be null");

		return connection.execute(
				client -> client.blmove(sourceKey, destinationKey, valueOf(from.name()), valueOf(to.name()), timeout),
				pipeline -> pipeline.blmove(sourceKey, destinationKey, valueOf(from.name()), valueOf(to.name()), timeout));
	}

	@Override
	public void lSet(byte @NonNull [] key, long index, byte @NonNull [] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		connection.executeStatus(client -> client.lset(key, index, value), pipeline -> pipeline.lset(key, index, value));
	}

	@Override
	public Long lRem(byte @NonNull [] key, long count, byte @NonNull [] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.execute(client -> client.lrem(key, count, value), pipeline -> pipeline.lrem(key, count, value));
	}

	@Override
	public byte[] lPop(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.lpop(key), pipeline -> pipeline.lpop(key));
	}

	@Override
	public List<byte[]> lPop(byte @NonNull [] key, long count) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.lpop(key, (int) count), pipeline -> pipeline.lpop(key, (int) count));
	}

	@Override
	public byte[] rPop(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.rpop(key), pipeline -> pipeline.rpop(key));
	}

	@Override
	public List<byte @NonNull []> rPop(byte @NonNull [] key, long count) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.rpop(key, (int) count), pipeline -> pipeline.rpop(key, (int) count));
	}

	@Override
	public List<byte @NonNull []> bLPop(int timeout, byte @NonNull []... keys) {

		Assert.notNull(keys, "Key must not be null");
		Assert.noNullElements(keys, "Keys must not contain null elements");

		return connection.execute(client -> client.blpop(timeout, keys), pipeline -> pipeline.blpop(timeout, keys));
	}

	@Override
	public List<byte @NonNull []> bRPop(int timeout, byte @NonNull []... keys) {

		Assert.notNull(keys, "Key must not be null");
		Assert.noNullElements(keys, "Keys must not contain null elements");

		return connection.execute(client -> client.brpop(timeout, keys), pipeline -> pipeline.brpop(timeout, keys));
	}

	@Override
	public byte[] rPopLPush(byte @NonNull [] srcKey, byte @NonNull [] dstKey) {

		Assert.notNull(srcKey, "Source key must not be null");
		Assert.notNull(dstKey, "Destination key must not be null");

		return connection.execute(client -> client.rpoplpush(srcKey, dstKey),
				pipeline -> pipeline.rpoplpush(srcKey, dstKey));
	}

	@Override
	public byte[] bRPopLPush(int timeout, byte @NonNull [] srcKey, byte @NonNull [] dstKey) {

		Assert.notNull(srcKey, "Source key must not be null");
		Assert.notNull(dstKey, "Destination key must not be null");

		return connection.execute(client -> client.brpoplpush(srcKey, dstKey, timeout),
				pipeline -> pipeline.brpoplpush(srcKey, dstKey, timeout));
	}

}
