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
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisJsonCommands;
import org.springframework.data.redis.connection.json.JsonPath;
import org.springframework.data.redis.connection.json.JsonValue;
import org.springframework.data.redis.serializer.GenericJacksonJsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisJsonSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Helper class that simplifies Redis JSON data access.
 *
 * @author Yordan Tsintsov
 * @since 4.2
 */
public class RedisJsonTemplate<K> extends RedisAccessor implements RedisJsonOperations<K> {

	private final RedisSerializer<K> keySerializer;
	private final RedisJsonSerializer jsonSerializer;

	/**
	 * Creates a new {@link RedisJsonTemplate} using the given {@link RedisConnectionFactory} and default serializers.
	 *
	 * @param connectionFactory must not be {@literal null}.
	 * @throws IllegalStateException if no supported JSON library is available on the classpath.
	 */
	@SuppressWarnings("unchecked")
	public RedisJsonTemplate(RedisConnectionFactory connectionFactory) {

		Assert.notNull(connectionFactory, "ConnectionFactory must not be null");

		setConnectionFactory(connectionFactory);
		keySerializer = (RedisSerializer<K>) RedisSerializer.java(getClass().getClassLoader());
		jsonSerializer = defaultJsonSerializer();
	}

	/**
	 * Creates a new {@link RedisJsonTemplate} using the given {@link RedisConnectionFactory} and serializers.
	 *
	 * @param connectionFactory must not be {@literal null}.
	 * @param keySerializer must not be {@literal null}.
	 * @param jsonSerializer must not be {@literal null}.
	 */
	public RedisJsonTemplate(RedisConnectionFactory connectionFactory, RedisSerializer<K> keySerializer, RedisJsonSerializer jsonSerializer) {

		Assert.notNull(connectionFactory, "ConnectionFactory must not be null");
		Assert.notNull(keySerializer, "KeySerializer must not be null");
		Assert.notNull(jsonSerializer, "JsonSerializer must not be null");

		setConnectionFactory(connectionFactory);
		this.keySerializer = keySerializer;
		this.jsonSerializer = jsonSerializer;
	}

	static RedisJsonSerializer defaultJsonSerializer() {

		if (!ClassUtils.isPresent("tools.jackson.databind.ObjectMapper", RedisJsonTemplate.class.getClassLoader())) {
			throw new IllegalStateException("No default RedisJsonSerializer available. Add Jackson 3 (tools.jackson) to the classpath, or use the constructor that accepts a RedisJsonSerializer");
		}

		return GenericJacksonJsonRedisSerializer.builder().build();
	}

	/**
	 * Returns the key serializer used by this template.
	 *
	 * @return the key serializer used by this template.
	 */
	public RedisSerializer<K> getKeySerializer() {
		return keySerializer;
	}

	/**
	 * Returns the JSON serializer used by this template.
	 *
	 * @return the JSON serializer used by this template
	 */
	public RedisJsonSerializer getJsonSerializer() {
		return jsonSerializer;
	}

	/**
	 * Executes the given action within a {@link RedisConnection} obtained from the configured
	 * {@link RedisConnectionFactory}, releasing the connection once the action completes.
	 *
	 * @param <T>    return type
	 * @param action callback object that specifies the Redis action; must not be {@literal null}.
	 * @return object returned by the action.
	 * @since 4.2
	 */
	private <T extends @Nullable Object> T execute(RedisCallback<T> action) {

		Assert.notNull(action, "Callback object must not be null");

		RedisConnectionFactory factory = getRequiredConnectionFactory();
		RedisConnection connection = RedisConnectionUtils.getConnection(factory);

		try {
			return action.doInRedis(connection);
		} finally {
			RedisConnectionUtils.releaseConnection(connection, factory);
		}
	}

	@Override
	public JsonArraySpec array(K key) {

		byte[] rawKey = rawKey(key);

		return new DefaultJsonArraySpec(this, rawKey);
	}

	@Override
	public JsonBooleanSpec bool(K key) {

		byte[] rawKey = rawKey(key);

		return new DefaultJsonBooleanSpec(this, rawKey);
	}

	@Override
	public JsonStringSpec string(K key) {

		byte[] rawKey = rawKey(key);

		return new DefaultJsonStringSpec(this, rawKey);
	}

	@Override
	public JsonAtKeySpec value(K key) {

		byte[] rawKey = rawKey(key);

		return new DefaultJsonAtKeySpec(this, rawKey);
	}

	@Override
	public JsonResult paths(K key, String... paths) {

		Assert.notEmpty(paths, "Paths must not be empty");

		byte[] rawKey = rawKey(key);

		JsonPath[] jsonPaths = new JsonPath[paths.length];
		for (int i = 0; i < paths.length; i++) {
			jsonPaths[i] = JsonPath.raw(paths[i]);
		}

		String response = this.execute(c -> c.jsonCommands().jsonGet(rawKey, jsonPaths));

		return new DefaultJsonResult(this.jsonSerializer, response);
	}

	@Override
	public JsonAtKeysSpec values(Collection<K> keys) {

		Assert.notEmpty(keys, "Keys must not be empty");

		byte[][] rawKeys = rawKeys(keys);

		return new DefaultJsonMultiGetSpec(this, rawKeys);
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
		final byte[] key;
		private JsonSetCondition condition = JsonSetCondition.upsert();

		DefaultJsonSpec(RedisJsonTemplate<?> template, byte[] key) {
			this.template = template;
			this.key = key;
		}

		// --- JsonKeySupport ---

		@Override
		public @Nullable Long clear() {
			return template.execute(c -> c.jsonCommands().jsonClear(key, JsonPath.raw(jsonPath)));
		}

		@Override
		public @Nullable Long delete() {
			return template.execute(c -> c.jsonCommands().jsonDel(key, JsonPath.raw(jsonPath)));
		}

		@Override
		public JsonResult get() {
			String result = template.execute(c -> c.jsonCommands().jsonGet(key, JsonPath.raw(jsonPath)));
			return new DefaultJsonResult(template.jsonSerializer, result);
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
			JsonValue jsonValue = JsonValue.raw(template.jsonSerializer.serializeAsString(value));
			return template.execute(c -> c.jsonCommands().jsonSet(key, JsonPath.raw(jsonPath), jsonValue, condition));
		}

	}

	static class DefaultJsonArraySpec extends DefaultPathSpec<JsonArraySpec> implements JsonArraySpec {

		private final RedisJsonTemplate<?> template;
		private final byte[] key;

		DefaultJsonArraySpec(RedisJsonTemplate<?> template, byte[] key) {
			this.template = template;
			this.key = key;
		}

		@Override
		public @Nullable List<@Nullable Long> append(Object... values) {
			JsonValue[] jsonValues = Arrays.stream(values).map(it -> JsonValue.raw(template.jsonSerializer.serializeAsString(it))).toArray(JsonValue[]::new);
			return template.execute(c -> c.jsonCommands().jsonArrAppend(key, JsonPath.raw(jsonPath), jsonValues));
		}

		@Override
		public @Nullable List<@Nullable Long> length() {
			return template.execute(c -> c.jsonCommands().jsonArrLen(key, JsonPath.raw(jsonPath)));
		}

		@Override
		public @Nullable List<@Nullable Long> trim(int start, int end) {
			return template.execute(c -> c.jsonCommands().jsonArrTrim(key, JsonPath.raw(jsonPath), start, end));
		}

		@Override
		public @Nullable List<Long> indexOf(Object value) {
			JsonValue jsonValue = JsonValue.raw(template.jsonSerializer.serializeAsString(value));
			return template.execute(c -> c.jsonCommands().jsonArrIndex(key, JsonPath.raw(jsonPath), jsonValue));
		}

		@Override
		public JsonArrayAtIndex index(int index) {
			return new DefaultJsonArrayAtIndex(template, key, jsonPath, index);
		}

	}

	static class DefaultJsonArrayAtIndex implements JsonArrayAtIndex {

		private final RedisJsonTemplate<?> template;
		private final byte[] key;
		private final String jsonPath;
		private final int index;

		DefaultJsonArrayAtIndex(RedisJsonTemplate<?> template, byte[] key, String jsonPath, int index) {
			this.template = template;
			this.key = key;
			this.jsonPath = jsonPath;
			this.index = index;
		}

		@Override
		public @Nullable List<@Nullable Long> insert(Object... values) {
			JsonValue[] jsonValues = Arrays.stream(values).map(it -> JsonValue.raw(template.jsonSerializer.serializeAsString(it))).toArray(JsonValue[]::new);
			return template.execute(c -> c.jsonCommands().jsonArrInsert(key, JsonPath.raw(jsonPath), index, jsonValues));
		}

	}

	static class DefaultJsonBooleanSpec extends DefaultJsonSpec<Boolean, JsonBooleanSpec> implements JsonBooleanSpec {

		DefaultJsonBooleanSpec(RedisJsonTemplate<?> template, byte[] key) {
			super(template, key);
		}

		@Override
		public @Nullable List<@Nullable Boolean> toggle() {
			return template.execute(c -> c.jsonCommands().jsonToggle(key, JsonPath.raw(jsonPath)));
		}

	}

	static class DefaultJsonStringSpec extends DefaultJsonSpec<String, JsonStringSpec> implements JsonStringSpec {

		DefaultJsonStringSpec(RedisJsonTemplate<?> template, byte[] key) {
			super(template, key);
		}

		@Override
		public @Nullable List<@Nullable Long> length() {
			return template.execute(c -> c.jsonCommands().jsonStrLen(key, JsonPath.raw(jsonPath)));
		}

		@Override
		public @Nullable List<@Nullable Long> append(String value) {
			return template.execute(c -> c.jsonCommands().jsonStrAppend(key, JsonPath.raw(jsonPath), value));
		}

	}

	static class DefaultJsonAtKeySpec extends DefaultJsonSpec<Object, JsonAtKeySpec> implements JsonAtKeySpec {

		DefaultJsonAtKeySpec(RedisJsonTemplate<?> template, byte[] key) {
			super(template, key);
		}

		@Override
		public @Nullable Boolean mergeWith(Object value) {
			JsonValue jsonValue = JsonValue.raw(template.jsonSerializer.serializeAsString(value));
			return template.execute(c -> c.jsonCommands().jsonMerge(key, JsonPath.raw(jsonPath), jsonValue));
		}

		@Override
		public @Nullable List<RedisJsonCommands.@Nullable JsonType> getType() {
			return template.execute(c -> c.jsonCommands().jsonType(key, JsonPath.raw(jsonPath)));
		}

	}

	static class DefaultJsonMultiGetSpec extends DefaultPathSpec<JsonAtKeysSpec> implements JsonAtKeysSpec {

		private final RedisJsonTemplate<?> template;
		private final byte[][] keys;

		DefaultJsonMultiGetSpec(RedisJsonTemplate<?> template, byte[][] keys) {
			this.template = template;
			this.keys = keys;
		}

		@Override
		public JsonResults get() {

			List<String> response = template.execute(c -> c.jsonCommands().jsonMGet(JsonPath.raw(jsonPath), keys));
			List<JsonResult> result = response == null ? null
					: response.stream().map(it -> (JsonResult) new DefaultJsonResult(template.jsonSerializer, it)).toList();

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
			return serializer.deserializeFromString(result, type);
		}

		@Override
		public <V> V as(ParameterizedTypeReference<V> type) {
			Assert.notNull(result, "Result must not be null");
			Assert.notNull(type, "Type must not be null");
			return serializer.deserializeFromString(result, type);
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
			return result == null | "null".equals(result);
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

	@SuppressWarnings("unchecked")
	private byte[] rawKey(Object key) {

		Assert.notNull(key, "non null key required");

		if (key instanceof byte[] bytes) {
			return bytes;
		}

		return keySerializer.serialize((K) key);
	}

	private byte[][] rawKeys(Collection<K> keys) {
		final byte[][] rawKeys = new byte[keys.size()][];

		int i = 0;
		for (K key : keys) {
			rawKeys[i++] = rawKey(key);
		}

		return rawKeys;
	}

}
