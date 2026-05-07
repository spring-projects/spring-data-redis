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
import io.lettuce.core.json.JsonValue;
import io.lettuce.core.json.arguments.JsonRangeArgs;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;

import org.springframework.data.redis.connection.RedisJsonCommands;
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
	public List<Long> jsonArrAppend(byte @NonNull [] key, @NonNull String path, @NonNull String @NonNull... values) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");
		Assert.notEmpty(values, "Values must not be empty");
		Assert.noNullElements(values, "Values must not be null");

		return connection.invoke().just(RedisJsonAsyncCommands::jsonArrappend, key, JsonPath.of(path), values);
	}

	@Override
	public List<Long> jsonArrIndex(byte @NonNull [] key, @NonNull String path, @NonNull String value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke().just(RedisJsonAsyncCommands::jsonArrindex, key, JsonPath.of(path), value);
	}

	@Override
	public List<Long> jsonArrInsert(byte @NonNull [] key, @NonNull String path, int index, @NonNull String @NonNull... values) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");
		Assert.notEmpty(values, "Values must not be empty");
		Assert.noNullElements(values, "Values must not be null");

		JsonValue[] jsonValues = Stream.of(values).map(LettuceConverters::toJsonValue).toArray(JsonValue[]::new);

		return connection.invoke().just(RedisJsonAsyncCommands::jsonArrinsert, key, JsonPath.of(path), index, jsonValues);
	}

	@Override
	public List<Long> jsonArrLen(byte @NonNull [] key, @NonNull String path) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");

		return connection.invoke().just(RedisJsonAsyncCommands::jsonArrlen, key, JsonPath.of(path));
	}

	@Override
	public List<Long> jsonArrTrim(byte @NonNull [] key, @NonNull String path, int start, int stop) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");

		JsonRangeArgs args = JsonRangeArgs.Builder.start(start).stop(stop);

		return connection.invoke().just(RedisJsonAsyncCommands::jsonArrtrim, key, JsonPath.of(path), args);
	}

	@Override
	public Long jsonClear(byte @NonNull [] key, @NonNull String path) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");

		return connection.invoke().just(RedisJsonAsyncCommands::jsonClear, key, JsonPath.of(path));
	}

	@Override
	public Long jsonDel(byte @NonNull [] key, @NonNull String path) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");

		return connection.invoke().just(RedisJsonAsyncCommands::jsonDel, key, JsonPath.of(path));
	}

	@Override
	public String jsonGet(byte @NonNull [] key, @NonNull String @NonNull... paths) {

		Assert.notNull(key, "Key must not be null");
		Assert.notEmpty(paths, "Paths must not be empty");
		Assert.noNullElements(paths, "Paths must not be null");

		JsonPath[] jsonPaths = Stream.of(paths).map(JsonPath::of).toArray(JsonPath[]::new);

		return connection.invoke().from(RedisJsonAsyncCommands::jsonGetRaw, key, jsonPaths)
				.get(it -> it.get(0) != null ? it.get(0) : null);
	}

	@Override
	public Boolean jsonMerge(byte @NonNull [] key, @NonNull String path, @NonNull String value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke().from(RedisJsonAsyncCommands::jsonMerge, key, JsonPath.of(path), value)
				.get(LettuceConverters::stringToBoolean);
	}

	@Override
	public List<String> jsonMGet(@NonNull String path, byte @NonNull [] @NonNull... keys) {

		Assert.notNull(path, "Path must not be null");
		Assert.notEmpty(keys, "Keys must not be empty");
		Assert.noNullElements(keys, "Keys must not be null");

		return connection.invoke().just(RedisJsonAsyncCommands::jsonMGetRaw, JsonPath.of(path), keys);
	}

	@Override
	public List<Number> jsonNumIncrBy(byte @NonNull [] key, @NonNull String path, @NonNull Number number) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");
		Assert.notNull(number, "Number must not be null");

		return connection.invoke().just(RedisJsonAsyncCommands::jsonNumincrby, key, JsonPath.of(path), number);
	}

	@Override
	public Boolean jsonSet(byte @NonNull [] key, @NonNull String path, @NonNull String value, @NonNull JsonSetOption option) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");
		Assert.notNull(value, "Value must not be null");
		Assert.notNull(option, "Option must not be null");

		return connection.invoke().from(RedisJsonAsyncCommands::jsonSet, key, JsonPath.of(path), value, LettuceConverters.toJsonSetArgs(option))
				.get(LettuceConverters::stringToBoolean);
	}

	@Override
	public List<Long> jsonStrAppend(byte @NonNull [] key, @NonNull String path, @NonNull String value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke().just(RedisJsonAsyncCommands::jsonStrappend, key, JsonPath.of(path), "\"" + value + "\"");
	}

	@Override
	public List<Long> jsonStrLen(byte @NonNull [] key, @NonNull String path) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");

		return connection.invoke().just(RedisJsonAsyncCommands::jsonStrlen, key, JsonPath.of(path));
	}

	@Override
	public List<Boolean> jsonToggle(byte @NonNull [] key, @NonNull String path) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");

		return connection.invoke().from(RedisJsonAsyncCommands::jsonToggle, key, JsonPath.of(path))
				.get(values -> values.stream().map(value -> value != null ? LettuceConverters.toBoolean(value) : null).toList());
	}

	@Override
	public List<JsonType> jsonType(byte @NonNull [] key, @NonNull String path) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");

		return connection.invoke().from(RedisJsonAsyncCommands::jsonType, key, JsonPath.of(path))
				.get(types -> types.stream().map(type -> type != null ? LettuceConverters.fromJsonType(type) : null).toList());
	}

}
