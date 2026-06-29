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
package org.springframework.data.redis.core;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import org.jspecify.annotations.Nullable;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.data.redis.connection.JsonSetCondition;
import org.springframework.data.redis.connection.RedisJsonCommands;
import org.springframework.data.redis.connection.json.JsonPath;
import org.springframework.data.redis.connection.json.JsonValue;
import org.springframework.data.redis.serializer.RedisJsonSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link JsonOperations}.
 *
 * @author Yordan Tsintsov
 * @since 4.2
 */
class DefaultJsonOperations<K> implements JsonOperations<K> {

	private final RedisJsonTemplate<K> template;

	DefaultJsonOperations(RedisJsonTemplate<K> template) {
		this.template = template;
	}

	@Override
	public JsonArraySpec array(K key) {

		byte[] rawKey = rawKey(key);

		return new DefaultJsonArraySpec(template, jsonSerializer(), rawKey);
	}

	@Override
	public JsonBooleanSpec bool(K key) {

		byte[] rawKey = rawKey(key);

		return new DefaultJsonBooleanSpec(template, jsonSerializer(), rawKey);
	}

	@Override
	public JsonStringSpec string(K key) {

		byte[] rawKey = rawKey(key);

		return new DefaultJsonStringSpec(template, jsonSerializer(), rawKey);
	}

	@Override
	public JsonAtKeySpec key(K key) {

		byte[] rawKey = rawKey(key);

		return new DefaultJsonAtKeySpec(template, jsonSerializer(), rawKey);
	}

	@Override
	public JsonResult paths(K key, String... paths) {

		Assert.notEmpty(paths, "Paths must not be empty");

		byte[] rawKey = rawKey(key);
		RedisJsonSerializer serializer = jsonSerializer();

		JsonPath[] jsonPaths = new JsonPath[paths.length];
		for (int i = 0; i < paths.length; i++) {
			jsonPaths[i] = JsonPath.of(paths[i]);
		}

		String response = template.execute(c -> c.jsonCommands().jsonGet(rawKey, jsonPaths));

		return new DefaultJsonResult(serializer, response);
	}

	@Override
	public JsonAtKeysSpec values(Collection<K> keys) {

		Assert.notEmpty(keys, "Keys must not be empty");

		byte[][] rawKeys = rawKeys(keys);

		return new DefaultJsonMultiGetSpec(template, jsonSerializer(), rawKeys);
	}

	private RedisJsonSerializer jsonSerializer() {
		RedisJsonSerializer serializer = template.getJsonSerializer();
		Assert.notNull(serializer, "JsonSerializer is not configured");
		return serializer;
	}

	private @Nullable RedisSerializer<K> keySerializer() {
		return template.getKeySerializer();
	}

	private RedisSerializer<K> requiredKeySerializer() {

		RedisSerializer<K> serializer = keySerializer();

		if (serializer == null) {
			throw new IllegalStateException("No key serializer configured");
		}

		return serializer;
	}

	@SuppressWarnings("unchecked")
	private byte[] rawKey(Object key) {

		Assert.notNull(key, "non null key required");

		if (key instanceof byte[] bytes) {
			return bytes;
		}

		return requiredKeySerializer().serialize((K) key);
	}

	private byte[][] rawKeys(Collection<K> keys) {
		final byte[][] rawKeys = new byte[keys.size()][];

		int i = 0;
		for (K key : keys) {
			rawKeys[i++] = rawKey(key);
		}

		return rawKeys;
	}

	static class DefaultPathSpec<P extends PathSpec<P>> implements PathSpec<P> {

		String jsonPath = JsonPath.root().asString();

		@Override
		@SuppressWarnings("unchecked")
		public P root() {
			this.jsonPath = JsonPath.root().asString();
			return (P) this;
		}

		@Override
		@SuppressWarnings("unchecked")
		public P path(String jsonPath) {
			Assert.hasText(jsonPath, "JsonPath must not be empty");
			this.jsonPath = jsonPath;
			return (P) this;
		}

	}

	abstract static class DefaultJsonSpec<T, S extends JsonKeySupport<S> & JsonSetSupport<T, S>>
			extends DefaultPathSpec<S> implements JsonKeySupport<S>, JsonSetSupport<T, S> {

		final RedisJsonTemplate<?> template;
		final RedisJsonSerializer serializer;
		final byte[] key;
		private JsonSetCondition condition = JsonSetCondition.upsert();

		DefaultJsonSpec(RedisJsonTemplate<?> template, RedisJsonSerializer serializer, byte[] key) {
			this.template = template;
			this.serializer = serializer;
			this.key = key;
		}

		// --- JsonKeySupport ---

		@Override
		public @Nullable Long clear() {
			return template.execute(c -> c.jsonCommands().jsonClear(key, JsonPath.of(jsonPath)));
		}

		@Override
		public @Nullable Long delete() {
			return template.execute(c -> c.jsonCommands().jsonDel(key, JsonPath.of(jsonPath)));
		}

		@Override
		public JsonResult get() {
			String result = template.execute(c -> c.jsonCommands().jsonGet(key, JsonPath.of(jsonPath)));
			return new DefaultJsonResult(serializer, result);
		}

		// --- JsonSetSupport ---

		@Override
		@SuppressWarnings("unchecked")
		public S conditional(Consumer<JsonSetSpec> consumer) {

			Assert.notNull(consumer, "Consumer must not be null");

			DefaultJsonSetSpec spec = new DefaultJsonSetSpec();
			consumer.accept(spec);
			this.condition = spec.condition();

			return (S) this;
		}

		@Override
		public @Nullable Boolean set(T value) {
			JsonValue jsonValue = JsonValue.raw(serializer.serialize(value));
			return template.execute(c -> c.jsonCommands().jsonSet(key, JsonPath.of(jsonPath), jsonValue, condition));
		}

	}

	static class DefaultJsonArraySpec extends DefaultPathSpec<JsonArraySpec> implements JsonArraySpec {

		private final RedisJsonTemplate<?> template;
		private final RedisJsonSerializer jsonSerializer;
		private final byte[] key;

		DefaultJsonArraySpec(RedisJsonTemplate<?> template, RedisJsonSerializer serializer, byte[] key) {
			this.template = template;
			this.jsonSerializer = serializer;
			this.key = key;
		}

		@Override
		public @Nullable List<@Nullable Long> append(Object... values) {
			JsonValue[] jsonValues = Arrays.stream(values).map(it -> JsonValue.raw(jsonSerializer.serialize(it))).toArray(JsonValue[]::new);
			return template.execute(c -> c.jsonCommands().jsonArrAppend(key, JsonPath.of(jsonPath), jsonValues));
		}

		@Override
		public @Nullable List<@Nullable Long> length() {
			return template.execute(c -> c.jsonCommands().jsonArrLen(key, JsonPath.of(jsonPath)));
		}

		@Override
		public @Nullable List<@Nullable Long> trim(int start, int end) {
			return template.execute(c -> c.jsonCommands().jsonArrTrim(key, JsonPath.of(jsonPath), start, end));
		}

		@Override
		public @Nullable List<Long> indexOf(Object value) {
			JsonValue jsonValue = JsonValue.raw(jsonSerializer.serialize(value));
			return template.execute(c -> c.jsonCommands().jsonArrIndex(key, JsonPath.of(jsonPath), jsonValue));
		}

		@Override
		public JsonArrayAtIndex index(int index) {
			return new DefaultJsonArrayAtIndex(template, jsonSerializer, key, jsonPath, index);
		}

	}

	static class DefaultJsonArrayAtIndex implements JsonArrayAtIndex {

		private final RedisJsonTemplate<?> template;
		private final RedisJsonSerializer serializer;
		private final byte[] key;
		private final String jsonPath;
		private final int index;

		DefaultJsonArrayAtIndex(RedisJsonTemplate<?> template, RedisJsonSerializer serializer, byte[] key, String jsonPath, int index) {
			this.template = template;
			this.serializer = serializer;
			this.key = key;
			this.jsonPath = jsonPath;
			this.index = index;
		}

		@Override
		public @Nullable List<@Nullable Long> insert(Object... values) {
			JsonValue[] jsonValues = Arrays.stream(values).map(it -> JsonValue.raw(serializer.serialize(it))).toArray(JsonValue[]::new);
			return template.execute(c -> c.jsonCommands().jsonArrInsert(key, JsonPath.of(jsonPath), index, jsonValues));
		}

	}

	static class DefaultJsonBooleanSpec extends DefaultJsonSpec<Boolean, JsonBooleanSpec> implements JsonBooleanSpec {

		DefaultJsonBooleanSpec(RedisJsonTemplate<?> template, RedisJsonSerializer serializer, byte[] key) {
			super(template, serializer, key);
		}

		@Override
		public @Nullable List<@Nullable Boolean> toggle() {
			return template.execute(c -> c.jsonCommands().jsonToggle(key, JsonPath.of(jsonPath)));
		}

	}

	static class DefaultJsonStringSpec extends DefaultJsonSpec<String, JsonStringSpec> implements JsonStringSpec {

		DefaultJsonStringSpec(RedisJsonTemplate<?> template, RedisJsonSerializer serializer, byte[] key) {
			super(template, serializer, key);
		}

		@Override
		public @Nullable List<@Nullable Long> length() {
			return template.execute(c -> c.jsonCommands().jsonStrLen(key, JsonPath.of(jsonPath)));
		}

		@Override
		public @Nullable List<@Nullable Long> append(String value) {
			return template.execute(c -> c.jsonCommands().jsonStrAppend(key, JsonPath.of(jsonPath), value));
		}

	}

	static class DefaultJsonAtKeySpec extends DefaultJsonSpec<Object, JsonAtKeySpec> implements JsonAtKeySpec {

		DefaultJsonAtKeySpec(RedisJsonTemplate<?> template, RedisJsonSerializer serializer, byte[] key) {
			super(template, serializer, key);
		}

		@Override
		public @Nullable Boolean mergeWith(Object value) {
			JsonValue jsonValue = JsonValue.raw(serializer.serialize(value));
			return template.execute(c -> c.jsonCommands().jsonMerge(key, JsonPath.of(jsonPath), jsonValue));
		}

		@Override
		public @Nullable List<RedisJsonCommands.@Nullable JsonType> getType() {
			return template.execute(c -> c.jsonCommands().jsonType(key, JsonPath.of(jsonPath)));
		}

	}

	static class DefaultJsonMultiGetSpec extends DefaultPathSpec<JsonAtKeysSpec> implements JsonAtKeysSpec {

		private final RedisJsonTemplate<?> template;
		private final RedisJsonSerializer serializer;
		private final byte[][] keys;

		DefaultJsonMultiGetSpec(RedisJsonTemplate<?> template, RedisJsonSerializer serializer, byte[][] keys) {
			this.template = template;
			this.serializer = serializer;
			this.keys = keys;
		}

		@Override
		public JsonResults get() {

			List<String> response = template.execute(c -> c.jsonCommands().jsonMGet(JsonPath.of(jsonPath), keys));
			List<JsonResult> result = response == null ? null
					: response.stream().map(it -> (JsonResult) new DefaultJsonResult(serializer, it)).toList();

			return new DefaultJsonResults(result);
		}

	}

	static class DefaultJsonSetSpec implements JsonSetSpec {

		private JsonSetCondition condition = JsonSetCondition.upsert();

		@Override
		public JsonSetSpec always() {
			this.condition = JsonSetCondition.upsert();
			return this;
		}

		@Override
		public JsonSetSpec ifAbsent() {
			this.condition = JsonSetCondition.ifPathNotExists();
			return this;
		}

		@Override
		public JsonSetSpec ifPresent() {
			this.condition = JsonSetCondition.ifPathExists();
			return this;
		}

		public JsonSetCondition condition() {
			return condition;
		}

	}

	static class DefaultJsonResult implements JsonResult {

		private final RedisJsonSerializer serializer;
		private final @Nullable String result;

		DefaultJsonResult(RedisJsonSerializer serializer, @Nullable String result) {
			this.serializer = serializer;
			this.result = result;
		}

		@Override
		public <V> V as(Class<V> type) {
			Assert.notNull(result, "Result must not be null");
			Assert.notNull(type, "Type must not be null");
			return serializer.deserialize(result, type);
		}

		@Override
		public <V> V as(ParameterizedTypeReference<V> type) {
			Assert.notNull(result, "Result must not be null");
			Assert.notNull(type, "Type must not be null");
			return serializer.deserialize(result, type);
		}

		@Override
		public String asString() {
			Assert.notNull(result, "Result must not be null");
			return result;
		}

		@Override
		public <U> U map(Function<? super byte[], ? extends U> mapper) {
			Assert.notNull(mapper, "Mapper must not be null");
			return mapper.apply(asBytes());
		}

		@Override
		public byte[] asBytes() {
			Assert.notNull(result, "Result must not be null");
			return result.getBytes(StandardCharsets.UTF_8);
		}

		@Override
		public boolean isNull() {
			if ("null".equals(result)) {
				return true;
			}
			return result == null;
		}

		@Override
		public @Nullable String toString() {
			return result;
		}

	}

	static class DefaultJsonResults implements JsonResults {

		private final @Nullable Collection<JsonResult> result;

		DefaultJsonResults(@Nullable Collection<JsonResult> result) {
			this.result = result;
		}

		@Override
		public <V> List<@Nullable V> as(Class<V> type) {
			Assert.notNull(result, "Result must not be null");
			Assert.notNull(type, "Type must not be null");
			return result.stream().map(it -> it.isNull() ? null : it.as(type)).toList();
		}

		@Override
		public <V> List<@Nullable V> as(ParameterizedTypeReference<V> type) {
			Assert.notNull(result, "Result must not be null");
			Assert.notNull(type, "Type must not be null");
			return result.stream().map(it -> it.isNull() ? null : it.as(type)).toList();
		}

		@Override
		public Iterator<JsonResult> iterator() {
			Assert.notNull(result, "Result must not be null");
			return result.iterator();
		}

		@Override
		public List<@Nullable String> asString() {
			Assert.notNull(result, "Result must not be null");
			return result.stream().map(it -> it.isNull() ? null : it.asString()).toList();
		}

		@Override
		public List<byte @Nullable []> asBytes() {
			Assert.notNull(result, "Result must not be null");
			return result.stream().map(it -> it.isNull() ? null : it.asBytes()).toList();
		}

		@Override
		public boolean isNull() {
			return result == null || result.isEmpty();
		}

	}

}
