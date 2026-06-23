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
package org.springframework.data.redis.connection.lettuce;

import java.util.List;
import java.util.stream.Stream;

import io.lettuce.core.api.async.RedisJsonAsyncCommands;
import io.lettuce.core.json.JsonPath;
import io.lettuce.core.json.arguments.JsonRangeArgs;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;

import org.springframework.data.redis.connection.JsonSetCondition;
import org.springframework.data.redis.connection.RedisJsonCommands;
import org.springframework.data.redis.connection.json.JsonValue;
import org.springframework.util.Assert;

/**
 * {@link RedisJsonCommands} implementation for Lettuce.
 *
 * @author Yordan Tsintsov
 * @since 4.2
 */
@NullUnmarked
class LettuceJsonCommands implements RedisJsonCommands {

	private final LettuceConnection connection;

	LettuceJsonCommands(LettuceConnection connection) {
		this.connection = connection;
	}

	@Override
	public List<Long> jsonArrAppend(byte @NonNull [] key, org.springframework.data.redis.connection.json.@NonNull JsonPath path, @NonNull JsonValue @NonNull... values) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");
		Assert.notEmpty(values, "Values must not be empty");
		Assert.noNullElements(values, "Values must not be null");

		String[] rawJsonValues = Stream.of(values).map(JsonValue::asString).toArray(String[]::new);

		return connection.invoke().just(RedisJsonAsyncCommands::jsonArrappend, key, JsonPath.of(path.asString()), rawJsonValues);
	}

	@Override
	public List<Long> jsonArrIndex(byte @NonNull [] key, org.springframework.data.redis.connection.json.@NonNull JsonPath path, @NonNull JsonValue value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke().just(RedisJsonAsyncCommands::jsonArrindex, key, JsonPath.of(path.asString()), value.asString());
	}

	@Override
	public List<Long> jsonArrInsert(byte @NonNull [] key, org.springframework.data.redis.connection.json.@NonNull JsonPath path, int index, @NonNull JsonValue @NonNull... values) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");
		Assert.notEmpty(values, "Values must not be empty");
		Assert.noNullElements(values, "Values must not be null");

		String[] rawJsonValues = Stream.of(values).map(JsonValue::asString).toArray(String[]::new);

		return connection.invoke().just(RedisJsonAsyncCommands::jsonArrinsert, key, JsonPath.of(path.asString()), index, rawJsonValues);
	}

	@Override
	public List<Long> jsonArrLen(byte @NonNull [] key, org.springframework.data.redis.connection.json.@NonNull JsonPath path) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");

		return connection.invoke().just(RedisJsonAsyncCommands::jsonArrlen, key, JsonPath.of(path.asString()));
	}

	@Override
	public List<Long> jsonArrTrim(byte @NonNull [] key, org.springframework.data.redis.connection.json.@NonNull JsonPath path, int start, int stop) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");

		JsonRangeArgs args = JsonRangeArgs.Builder.start(start).stop(stop);

		return connection.invoke().just(RedisJsonAsyncCommands::jsonArrtrim, key, JsonPath.of(path.asString()), args);
	}

	@Override
	public Long jsonClear(byte @NonNull [] key, org.springframework.data.redis.connection.json.@NonNull JsonPath path) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");

		return connection.invoke().just(RedisJsonAsyncCommands::jsonClear, key, JsonPath.of(path.asString()));
	}

	@Override
	public Long jsonDel(byte @NonNull [] key, org.springframework.data.redis.connection.json.@NonNull JsonPath path) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");

		return connection.invoke().just(RedisJsonAsyncCommands::jsonDel, key, JsonPath.of(path.asString()));
	}

	@Override
	public String jsonGet(byte @NonNull [] key, org.springframework.data.redis.connection.json.@NonNull JsonPath @NonNull... paths) {

		Assert.notNull(key, "Key must not be null");
		Assert.notEmpty(paths, "Paths must not be empty");
		Assert.noNullElements(paths, "Paths must not be null");

		JsonPath[] jsonPaths = Stream.of(paths).map(path -> JsonPath.of(path.asString())).toArray(JsonPath[]::new);

		return connection.invoke().from(RedisJsonAsyncCommands::jsonGetRaw, key, jsonPaths)
				.get(it -> it.get(0) != null ? it.get(0) : null);
	}

	@Override
	public Boolean jsonMerge(byte @NonNull [] key, org.springframework.data.redis.connection.json.@NonNull JsonPath path, @NonNull JsonValue value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke().from(RedisJsonAsyncCommands::jsonMerge, key, JsonPath.of(path.asString()), value.asString())
				.get(LettuceConverters::stringToBoolean);
	}

	@Override
	public List<String> jsonMGet(org.springframework.data.redis.connection.json.@NonNull JsonPath path, byte @NonNull [] @NonNull... keys) {

		Assert.notNull(path, "Path must not be null");
		Assert.notEmpty(keys, "Keys must not be empty");
		Assert.noNullElements(keys, "Keys must not be null");

		return connection.invoke().just(RedisJsonAsyncCommands::jsonMGetRaw, JsonPath.of(path.asString()), keys);
	}

	@Override
	public Boolean jsonSet(byte @NonNull [] key, org.springframework.data.redis.connection.json.@NonNull JsonPath path, @NonNull JsonValue value, @NonNull JsonSetCondition condition) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");
		Assert.notNull(value, "Value must not be null");
		Assert.notNull(condition, "Option must not be null");

		return connection.invoke().from(RedisJsonAsyncCommands::jsonSet, key, JsonPath.of(path.asString()), value.asString(), LettuceConverters.toJsonSetArgs(condition))
				.get(LettuceConverters::stringToBoolean);
	}

	@Override
	public List<Long> jsonStrAppend(byte @NonNull [] key, org.springframework.data.redis.connection.json.@NonNull JsonPath path, @NonNull String value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke().just(RedisJsonAsyncCommands::jsonStrappend, key, JsonPath.of(path.asString()), "\"" + value + "\"");
	}

	@Override
	public List<Long> jsonStrLen(byte @NonNull [] key, org.springframework.data.redis.connection.json.@NonNull JsonPath path) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");

		return connection.invoke().just(RedisJsonAsyncCommands::jsonStrlen, key, JsonPath.of(path.asString()));
	}

	@Override
	public List<Boolean> jsonToggle(byte @NonNull [] key, org.springframework.data.redis.connection.json.@NonNull JsonPath path) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");

		return connection.invoke().from(RedisJsonAsyncCommands::jsonToggle, key, JsonPath.of(path.asString()))
				.get(values -> values.stream().map(value -> value != null ? LettuceConverters.toBoolean(value) : null).toList());
	}

	@Override
	public List<JsonType> jsonType(byte @NonNull [] key, org.springframework.data.redis.connection.json.@NonNull JsonPath path) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");

		return connection.invoke().from(RedisJsonAsyncCommands::jsonType, key, JsonPath.of(path.asString()))
				.get(types -> types.stream().map(type -> type != null ? LettuceConverters.fromJsonType(type) : null).toList());
	}

}
