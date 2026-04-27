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

import org.jspecify.annotations.Nullable;
import org.springframework.data.redis.connection.RedisJsonCommands;
import org.springframework.util.Assert;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.json.Path2;
import redis.clients.jedis.json.commands.RedisJsonPipelineCommands;

import java.util.List;
import java.util.stream.Stream;

/**
 * {@link RedisJsonCommands} implementation for Jedis.
 *
 * @author Yordan Tsintsov
 * @since 4.1
 */
class JedisJsonCommands implements RedisJsonCommands {

	private final JedisConnection connection;

	JedisJsonCommands(JedisConnection connection) {
		this.connection = connection;
	}

	@Override
	public List<@Nullable Long> jsonArrAppend(byte[] key, String path, String... values) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");
		Assert.notEmpty(values, "Values must not be empty");
		Assert.noNullElements(values, "Values must not be null");

		return connection.invoke().just(UnifiedJedis::jsonArrAppend, RedisJsonPipelineCommands::jsonArrAppend, new String(key), Path2.of(path), values);
	}

	@Override
	public List<@Nullable Long> jsonArrIndex(byte[] key, String path, String value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke().just(UnifiedJedis::jsonArrIndex, RedisJsonPipelineCommands::jsonArrIndex, new String(key), Path2.of(path), value);
	}

	@Override
	public List<@Nullable Long> jsonArrInsert(byte[] key, String path, int index, String... values) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");
		Assert.notEmpty(values, "Values must not be empty");
		Assert.noNullElements(values, "Values must not be null");

		return connection.invoke().just(UnifiedJedis::jsonArrInsert, RedisJsonPipelineCommands::jsonArrInsert, new String(key), Path2.of(path), index, values);
	}

	@Override
	public List<@Nullable Long> jsonArrLen(byte[] key, String path) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");

		return connection.invoke().just(UnifiedJedis::jsonArrLen, RedisJsonPipelineCommands::jsonArrLen, new String(key), Path2.of(path));
	}

	@Override
	public List<@Nullable Long> jsonArrTrim(byte[] key, String path, int start, int stop) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");

		return connection.invoke().just(UnifiedJedis::jsonArrTrim, RedisJsonPipelineCommands::jsonArrTrim, new String(key), Path2.of(path), start, stop);
	}

	@Override
	public Long jsonClear(byte[] key, String path) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");

		return connection.invoke().just(UnifiedJedis::jsonClear, RedisJsonPipelineCommands::jsonClear, new String(key), Path2.of(path));
	}

	@Override
	public Long jsonDel(byte[] key, String path) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");

		return connection.invoke().just(UnifiedJedis::jsonDel, RedisJsonPipelineCommands::jsonDel, new String(key), Path2.of(path));
	}

	@Override
	public @Nullable String jsonGet(byte[] key, String... paths) {

		Assert.notNull(key, "Key must not be null");
		Assert.notEmpty(paths, "Paths must not be empty");
		Assert.noNullElements(paths, "Paths must not be null");

		Path2[] path2s = Stream.of(paths).map(Path2::of).toArray(Path2[]::new);

		return connection.invoke().from(UnifiedJedis::jsonGet, RedisJsonPipelineCommands::jsonGet, new String(key), path2s)
				.get(Object::toString);
	}

	@Override
	public Boolean jsonMerge(byte[] key, String path, String value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke().from(UnifiedJedis::jsonMerge, RedisJsonPipelineCommands::jsonMerge, new String(key), Path2.of(path), value)
				.get(JedisConverters::stringToBoolean);
	}

	@Override
	public List<@Nullable String> jsonMGet(String path, byte[]... keys) {

		Assert.notNull(path, "Path must not be null");
		Assert.notEmpty(keys, "Keys must not be empty");
		Assert.noNullElements(keys, "Keys must not be null");

		String[] stringKeys = Stream.of(keys).map(String::new).toArray(String[]::new);

		return connection.invoke().from(UnifiedJedis::jsonMGet, RedisJsonPipelineCommands::jsonMGet, Path2.of(path), stringKeys)
				.get(jsonArrList -> jsonArrList.stream().map(arr -> arr != null ? arr.toString() : null).toList());
	}

	@Override
	public List<@Nullable Number> jsonNumIncrBy(byte[] key, String path, Number number) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");
		Assert.notNull(number, "Number must not be null");

		return connection.invoke().from(UnifiedJedis::jsonNumIncrBy, RedisJsonPipelineCommands::jsonNumIncrBy, new String(key), Path2.of(path), number.doubleValue())
				.get(JedisConverters::fromJsonNumberListObject);
	}

	@Override
	public Boolean jsonSet(byte[] key, String path, String value, JsonSetOption option) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke().from(UnifiedJedis::jsonSet, RedisJsonPipelineCommands::jsonSet, new String(key), Path2.of(path), value, JedisConverters.toJsonSetParams(option))
				.get(JedisConverters::stringToBoolean);
	}

	@Override
	public List<@Nullable Long> jsonStrAppend(byte[] key, String path, String value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke().just(UnifiedJedis::jsonStrAppend, RedisJsonPipelineCommands::jsonStrAppend, new String(key), Path2.of(path), value);
	}

	@Override
	public List<@Nullable Long> jsonStrLen(byte[] key, String path) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");

		return connection.invoke().just(UnifiedJedis::jsonStrLen, RedisJsonPipelineCommands::jsonStrLen, new String(key), Path2.of(path));
	}

	@Override
	public List<@Nullable Boolean> jsonToggle(byte[] key, String path) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");

		return connection.invoke().just(UnifiedJedis::jsonToggle, RedisJsonPipelineCommands::jsonToggle, new String(key), Path2.of(path));
	}

	@Override
	public List<@Nullable JsonType> jsonType(byte[] key, String path) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");

		return connection.invoke().from(UnifiedJedis::jsonType, RedisJsonPipelineCommands::jsonType, new String(key), Path2.of(path))
				.get(types -> types.stream().map(type -> type != null ? JedisConverters.fromJsonType(type) : null).toList());
	}

}
