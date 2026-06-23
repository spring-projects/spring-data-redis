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

import java.util.List;
import java.util.stream.Stream;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.springframework.data.redis.connection.JsonSetCondition;
import org.springframework.data.redis.connection.json.JsonPath;
import org.springframework.data.redis.connection.json.JsonValue;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.json.Path2;
import redis.clients.jedis.json.commands.RedisJsonPipelineCommands;

import org.springframework.data.redis.connection.RedisJsonCommands;
import org.springframework.util.Assert;

/**
 * {@link RedisJsonCommands} implementation for Jedis.
 *
 * @author Yordan Tsintsov
 * @since 4.2
 */
@NullUnmarked
class JedisJsonCommands implements RedisJsonCommands {

	private final JedisConnection connection;

	JedisJsonCommands(JedisConnection connection) {
		this.connection = connection;
	}

	@Override
	public List<Long> jsonArrAppend(byte @NonNull [] key, @NonNull JsonPath path, @NonNull JsonValue @NonNull... values) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");
		Assert.notEmpty(values, "Values must not be empty");
		Assert.noNullElements(values, "Values must not be null");

		String[] rawJsonValues = Stream.of(values).map(JsonValue::asString).toArray(String[]::new);

		return connection.invoke().just(UnifiedJedis::jsonArrAppend, RedisJsonPipelineCommands::jsonArrAppend, JedisConverters.toString(key), Path2.of(path.asString()), rawJsonValues);
	}

	@Override
	public List<Long> jsonArrIndex(byte @NonNull [] key, @NonNull JsonPath path, @NonNull JsonValue value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke().just(UnifiedJedis::jsonArrIndex, RedisJsonPipelineCommands::jsonArrIndex, JedisConverters.toString(key), Path2.of(path.asString()), value.asString());
	}

	@Override
	public List<Long> jsonArrInsert(byte @NonNull [] key, @NonNull JsonPath path, int index, @NonNull JsonValue @NonNull... values) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");
		Assert.notEmpty(values, "Values must not be empty");
		Assert.noNullElements(values, "Values must not be null");

		String[] rawJsonValues = Stream.of(values).map(JsonValue::asString).toArray(String[]::new);

		return connection.invoke().just(UnifiedJedis::jsonArrInsert, RedisJsonPipelineCommands::jsonArrInsert, JedisConverters.toString(key), Path2.of(path.asString()), index, rawJsonValues);
	}

	@Override
	public List<Long> jsonArrLen(byte @NonNull [] key, @NonNull JsonPath path) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");

		return connection.invoke().just(UnifiedJedis::jsonArrLen, RedisJsonPipelineCommands::jsonArrLen, JedisConverters.toString(key), Path2.of(path.asString()));
	}

	@Override
	public List<Long> jsonArrTrim(byte @NonNull [] key, @NonNull JsonPath path, int start, int stop) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");

		return connection.invoke().just(UnifiedJedis::jsonArrTrim, RedisJsonPipelineCommands::jsonArrTrim, JedisConverters.toString(key), Path2.of(path.asString()), start, stop);
	}

	@Override
	public Long jsonClear(byte @NonNull [] key, @NonNull JsonPath path) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");

		return connection.invoke().just(UnifiedJedis::jsonClear, RedisJsonPipelineCommands::jsonClear, JedisConverters.toString(key), Path2.of(path.asString()));
	}

	@Override
	public Long jsonDel(byte @NonNull [] key, @NonNull JsonPath path) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");

		return connection.invoke().just(UnifiedJedis::jsonDel, RedisJsonPipelineCommands::jsonDel, JedisConverters.toString(key), Path2.of(path.asString()));
	}

	@Override
	public String jsonGet(byte @NonNull [] key, @NonNull JsonPath @NonNull... paths) {

		Assert.notNull(key, "Key must not be null");
		Assert.notEmpty(paths, "Paths must not be empty");
		Assert.noNullElements(paths, "Paths must not be null");

		Path2[] path2s = Stream.of(paths).map(path -> Path2.of(path.asString())).toArray(Path2[]::new);

		return connection.invoke().from(UnifiedJedis::jsonGet, RedisJsonPipelineCommands::jsonGet, JedisConverters.toString(key), path2s)
				.get(Object::toString);
	}

	@Override
	public Boolean jsonMerge(byte @NonNull [] key, @NonNull JsonPath path, @NonNull JsonValue value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke().from(UnifiedJedis::jsonMerge, RedisJsonPipelineCommands::jsonMerge, JedisConverters.toString(key), Path2.of(path.asString()), value.asString())
				.get(JedisConverters::stringToBoolean);
	}

	@Override
	public List<String> jsonMGet(@NonNull JsonPath path, byte @NonNull [] @NonNull... keys) {

		Assert.notNull(path, "Path must not be null");
		Assert.notEmpty(keys, "Keys must not be empty");
		Assert.noNullElements(keys, "Keys must not be null");

		String[] stringKeys = Stream.of(keys).map(String::new).toArray(String[]::new);

		return connection.invoke().from(UnifiedJedis::jsonMGet, RedisJsonPipelineCommands::jsonMGet, Path2.of(path.asString()), stringKeys)
				.get(jsonArrList -> jsonArrList.stream().map(arr -> arr != null ? arr.toString() : null).toList());
	}

	@Override
	public Boolean jsonSet(byte @NonNull [] key, @NonNull JsonPath path, @NonNull JsonValue value, @NonNull JsonSetCondition condition) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");
		Assert.notNull(value, "Value must not be null");
		Assert.notNull(condition, "Condition must not be null");

		return connection.invoke().from(UnifiedJedis::jsonSet, RedisJsonPipelineCommands::jsonSet, JedisConverters.toString(key), Path2.of(path.asString()), value.asString(), JedisConverters.toJsonSetParams(condition))
				.get(JedisConverters::stringToBoolean);
	}

	@Override
	public List<Long> jsonStrAppend(byte @NonNull [] key, @NonNull JsonPath path, @NonNull String value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke().just(UnifiedJedis::jsonStrAppend, RedisJsonPipelineCommands::jsonStrAppend, JedisConverters.toString(key), Path2.of(path.asString()), value);
	}

	@Override
	public List<Long> jsonStrLen(byte @NonNull [] key, @NonNull JsonPath path) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");

		return connection.invoke().just(UnifiedJedis::jsonStrLen, RedisJsonPipelineCommands::jsonStrLen, JedisConverters.toString(key), Path2.of(path.asString()));
	}

	@Override
	public List<Boolean> jsonToggle(byte @NonNull [] key, @NonNull JsonPath path) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");

		return connection.invoke().just(UnifiedJedis::jsonToggle, RedisJsonPipelineCommands::jsonToggle, JedisConverters.toString(key), Path2.of(path.asString()));
	}

	@Override
	public List<JsonType> jsonType(byte @NonNull [] key, @NonNull JsonPath path) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");

		return connection.invoke().from(UnifiedJedis::jsonType, RedisJsonPipelineCommands::jsonType, JedisConverters.toString(key), Path2.of(path.asString()))
				.get(types -> types.stream().map(type -> type != null ? JedisConverters.fromJsonType(type) : null).toList());
	}

}
