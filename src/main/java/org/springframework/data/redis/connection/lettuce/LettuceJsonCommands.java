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

import io.lettuce.core.api.async.RedisJsonAsyncCommands;
import io.lettuce.core.json.JsonArray;
import io.lettuce.core.json.JsonObject;
import io.lettuce.core.json.JsonPath;
import io.lettuce.core.json.arguments.JsonRangeArgs;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Stream;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;

import org.springframework.data.redis.connection.RedisJsonCommands;
import org.springframework.data.redis.connection.json.JsonSetCondition;
import org.springframework.data.redis.connection.json.JsonType;
import org.springframework.data.redis.connection.json.JsonValue;
import org.springframework.data.redis.util.ByteUtils;
import org.springframework.util.Assert;

/**
 * {@link RedisJsonCommands} implementation for Lettuce.
 *
 * @author Yordan Tsintsov
 * @author Mark Paluch
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

		io.lettuce.core.json.JsonValue[] lettuceValues = Stream.of(values).map(LettuceJsonCommands::toJsonValue)
				.toArray(io.lettuce.core.json.JsonValue[]::new);

		return connection.invoke().just(RedisJsonAsyncCommands::jsonArrappend, key, toPath(path), lettuceValues);
	}

	@Override
	public List<Long> jsonArrIndex(byte @NonNull [] key, org.springframework.data.redis.connection.json.@NonNull JsonPath path, @NonNull JsonValue value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke().just(RedisJsonAsyncCommands::jsonArrindex, key, toPath(path),
				toJsonValue(value));
	}

	@Override
	public List<Long> jsonArrInsert(byte @NonNull [] key, org.springframework.data.redis.connection.json.@NonNull JsonPath path, int index, @NonNull JsonValue @NonNull... values) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");
		Assert.notEmpty(values, "Values must not be empty");
		Assert.noNullElements(values, "Values must not be null");

		io.lettuce.core.json.JsonValue[] lettuceValues = Stream.of(values).map(LettuceJsonCommands::toJsonValue)
				.toArray(io.lettuce.core.json.JsonValue[]::new);

		return connection.invoke().just(RedisJsonAsyncCommands::jsonArrinsert, key, toPath(path), index, lettuceValues);
	}

	@Override
	public List<Long> jsonArrLen(byte @NonNull [] key, org.springframework.data.redis.connection.json.@NonNull JsonPath path) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");

		return connection.invoke().just(RedisJsonAsyncCommands::jsonArrlen, key, toPath(path));
	}

	@Override
	public List<Long> jsonArrTrim(byte @NonNull [] key, org.springframework.data.redis.connection.json.@NonNull JsonPath path, int start, int stop) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");

		JsonRangeArgs args = JsonRangeArgs.Builder.start(start).stop(stop);

		return connection.invoke().just(RedisJsonAsyncCommands::jsonArrtrim, key, toPath(path), args);
	}

	@Override
	public Long jsonClear(byte @NonNull [] key, org.springframework.data.redis.connection.json.@NonNull JsonPath path) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");

		return connection.invoke().just(RedisJsonAsyncCommands::jsonClear, key, toPath(path));
	}

	@Override
	public Long jsonDel(byte @NonNull [] key, org.springframework.data.redis.connection.json.@NonNull JsonPath path) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");

		return connection.invoke().just(RedisJsonAsyncCommands::jsonDel, key, toPath(path));
	}

	@Override
	public byte[] jsonGet(byte @NonNull [] key, org.springframework.data.redis.connection.json.@NonNull JsonPath @NonNull... paths) {

		Assert.notNull(key, "Key must not be null");
		Assert.notEmpty(paths, "Paths must not be empty");
		Assert.noNullElements(paths, "Paths must not be null");

		JsonPath[] jsonPaths = Stream.of(paths).map(this::toPath).toArray(JsonPath[]::new);

		return connection.invoke().from(RedisJsonAsyncCommands::jsonGet, key, jsonPaths).get(it -> {

			io.lettuce.core.json.JsonValue value = it.get(0);
			ByteBuffer buffer = value == null ? null : value.asByteBuffer();

			return buffer == null ? null : ByteUtils.getBytes(buffer);
		});
	}

	@Override
	public Boolean jsonMerge(byte @NonNull [] key, org.springframework.data.redis.connection.json.@NonNull JsonPath path, @NonNull JsonValue value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke()
				.from(RedisJsonAsyncCommands::jsonMerge, key, toPath(path), toJsonValue(value))
				.getOrElse(LettuceConverters::stringToBoolean, () -> false);
	}

	@Override
	public List<byte[]> jsonMGet(org.springframework.data.redis.connection.json.@NonNull JsonPath path, byte @NonNull [] @NonNull... keys) {

		Assert.notNull(path, "Path must not be null");
		Assert.notEmpty(keys, "Keys must not be empty");
		Assert.noNullElements(keys, "Keys must not be null");

		return connection.invoke().from(RedisJsonAsyncCommands::jsonMGet, toPath(path), keys)
				.get(result -> result.stream().map(it -> {

					ByteBuffer buffer = it == null ? null : it.asByteBuffer();
					return buffer == null ? null : ByteUtils.getBytes(buffer);
				}).toList());
	}

	@Override
	public Boolean jsonSet(byte @NonNull [] key, org.springframework.data.redis.connection.json.@NonNull JsonPath path, @NonNull JsonValue value, @NonNull JsonSetCondition condition) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");
		Assert.notNull(value, "Value must not be null");
		Assert.notNull(condition, "Option must not be null");

		return connection.invoke().from(RedisJsonAsyncCommands::jsonSet, key, toPath(path), toJsonValue(value),
				LettuceConverters.toJsonSetArgs(condition)).getOrElse(LettuceConverters::stringToBoolean, () -> false);
	}

	@Override
	public List<Long> jsonStrAppend(byte @NonNull [] key, org.springframework.data.redis.connection.json.@NonNull JsonPath path, @NonNull String value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke().just(RedisJsonAsyncCommands::jsonStrappend, key, toPath(path),
				toJsonValue(JsonValue.of(value)));
	}

	@Override
	public List<Long> jsonStrLen(byte @NonNull [] key, org.springframework.data.redis.connection.json.@NonNull JsonPath path) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");

		return connection.invoke().just(RedisJsonAsyncCommands::jsonStrlen, key, toPath(path));
	}

	@Override
	public List<Boolean> jsonToggle(byte @NonNull [] key, org.springframework.data.redis.connection.json.@NonNull JsonPath path) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");

		return connection.invoke().from(RedisJsonAsyncCommands::jsonToggle, key, toPath(path))
				.get(values -> values.stream().map(value -> value != null ? LettuceConverters.toBoolean(value) : null).toList());
	}

	@Override
	public List<JsonType> jsonType(byte @NonNull [] key, org.springframework.data.redis.connection.json.@NonNull JsonPath path) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(path, "Path must not be null");

		return connection.invoke().from(RedisJsonAsyncCommands::jsonType, key, toPath(path))
				.get(types -> types.stream().map(LettuceConverters::fromJsonType).toList());
	}

	private JsonPath toPath(org.springframework.data.redis.connection.json.JsonPath path) {
		return JsonPath.of(path.asString());
	}

	private static io.lettuce.core.json.JsonValue toJsonValue(@NonNull JsonValue jsonValue) {

		byte[] bytes = jsonValue.asBytes();
		return new io.lettuce.core.json.JsonValue() {

			@Override
			public ByteBuffer asByteBuffer() {
				return ByteBuffer.wrap(bytes);
			}

			@Override
			public boolean isJsonArray() {
				return false;
			}

			@Override
			public JsonArray asJsonArray() {
				return null;
			}

			@Override
			public boolean isJsonObject() {
				return false;
			}

			@Override
			public JsonObject asJsonObject() {
				return null;
			}

			@Override
			public boolean isString() {
				return false;
			}

			@Override
			public String asString() {
				return jsonValue.asString();
			}

			@Override
			public boolean isNumber() {
				return false;
			}

			@Override
			public Number asNumber() {
				return null;
			}

			@Override
			public boolean isBoolean() {
				return false;
			}

			@Override
			public Boolean asBoolean() {
				return null;
			}

			@Override
			public boolean isNull() {
				return false;
			}

			@Override
			public <T> T toObject(Class<T> type) {
				return null;
			}
		};
	}

}
