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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;

import org.jspecify.annotations.Nullable;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.ResolvableType;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisJsonCommands;
import org.springframework.data.redis.connection.json.JsonPath;
import org.springframework.data.redis.connection.json.JsonSetCondition;
import org.springframework.data.redis.connection.json.JsonType;
import org.springframework.data.redis.connection.json.JsonValue;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.GenericJacksonJsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisJsonSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.SerializationException;
import org.springframework.data.redis.util.ByteUtils;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;

/**
 * <b>This is the central JSON entrypoint in the Redis core package for flexible JSON result consumption.</b> It can be
 * used directly for many data access purposes, supporting any kind of Redis JSON operation.
 * <p>
 * Typed entry points bound to a key allow configuring the command and running it by calling a terminal method returning
 * the command result, for example:
 *
 * <pre class="code">
 * operations.value("key").set("value");
 * operations.value("key").path("$..name").setIfAbsent("Doe");
 * operations.array("key").path("$.names").index(2).insert("John");
 *
 * Person person = operations.value("key").get().as(Person.class);
 * </pre>
 * <p>
 * Specification objects are immutable and can be used to build up complex queries.
 *
 * @author Yordan Tsintsov
 * @author Mark Paluch
 * @since 4.2
 * @param <K> the Redis key type.
 */
public class RedisJsonTemplate<K> implements RedisJsonOperations<K> {

	/**
	 * Matches a bare property path such as {@code name} or {@code address.city} and rejects double dots.
	 */
	private static final Pattern BARE_PROPERTY_PATH = Pattern.compile("[\\w-]+(?:\\.[\\w-]+)*",
			Pattern.UNICODE_CHARACTER_CLASS);

	private final RedisConnectionFactory connectionFactory;

	private final RedisSerializer<K> keySerializer;

	private final RedisJsonSerializer jsonSerializer;

	/**
	 * Creates a new {@link RedisJsonTemplate} using the given {@link RedisConnectionFactory} and serializers.
	 *
	 * @param connectionFactory must not be {@literal null}.
	 * @param keySerializer must not be {@literal null}.
	 * @param jsonSerializer must not be {@literal null}.
	 */
	public RedisJsonTemplate(RedisConnectionFactory connectionFactory, RedisSerializer<K> keySerializer,
			RedisJsonSerializer jsonSerializer) {

		Assert.notNull(connectionFactory, "ConnectionFactory must not be null");
		Assert.notNull(keySerializer, "KeySerializer must not be null");
		Assert.notNull(jsonSerializer, "JsonSerializer must not be null");

		this.connectionFactory = connectionFactory;
		this.keySerializer = keySerializer;
		this.jsonSerializer = jsonSerializer;
	}

	static RedisJsonSerializer defaultJsonSerializer() {

		if (ClassUtils.isPresent("tools.jackson.databind.ObjectMapper", RedisJsonTemplate.class.getClassLoader())) {
			return GenericJacksonJsonRedisSerializer.builder().build();
		}
		if (ClassUtils.isPresent("com.fasterxml.jackson.databind.ObjectMapper", RedisJsonTemplate.class.getClassLoader())) {
			return GenericJackson2JsonRedisSerializer.builder().defaultTyping(false).build();
		}
		throw new IllegalStateException(
				"No default RedisJsonSerializer available. Add Jackson 2 (com.fasterxml) or 3 (tools.jackson) to the classpath, or provide a RedisJsonSerializer");
	}

	/**
	 * Create a new {@link RedisJsonTemplate} using the given {@link RedisConnectionFactory} and default serializers for
	 * usage with {@link String} keys.
	 *
	 * @param connectionFactory the connection factory to use.
	 * @return a new {@link RedisJsonTemplate} instance.
	 * @see #create(RedisConnectionFactory, RedisJsonSerializer)
	 * @see RedisJsonTemplate#RedisJsonTemplate(RedisConnectionFactory, RedisSerializer, RedisJsonSerializer)
	 */
	public static RedisJsonTemplate<String> create(RedisConnectionFactory connectionFactory) {
		return create(connectionFactory, defaultJsonSerializer());
	}

	/**
	 * Create a new {@link RedisJsonTemplate} using the given {@link RedisConnectionFactory} and
	 * {@link RedisJsonSerializer} for usage with {@link String} keys.
	 *
	 * @param connectionFactory the connection factory to use.
	 * @param jsonSerializer the JSON serializer to use.
	 * @return a new {@link RedisJsonTemplate} instance.
	 * @see #create(RedisConnectionFactory)
	 * @see RedisJsonTemplate#RedisJsonTemplate(RedisConnectionFactory, RedisSerializer, RedisJsonSerializer)
	 */
	public static RedisJsonTemplate<String> create(RedisConnectionFactory connectionFactory,
			RedisJsonSerializer jsonSerializer) {
		return new RedisJsonTemplate<>(connectionFactory, RedisSerializer.string(), jsonSerializer);
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

	private <T extends @Nullable Object> T execute(Function<RedisJsonCommands, T> action) {

		RedisConnection connection = RedisConnectionUtils.getConnection(connectionFactory);

		try {
			return action.apply(connection.jsonCommands());
		} finally {
			RedisConnectionUtils.releaseConnection(connection, connectionFactory);
		}
	}

	@Override
	public JsonArraySpec array(K key) {
		return new DefaultJsonArraySpec(rawKey(key), JsonPath.root());
	}

	@Override
	public JsonBooleanSpec bool(K key) {
		return new DefaultJsonBooleanSpec(rawKey(key), JsonPath.root(), JsonSetCondition.upsert());
	}

	@Override
	public JsonStringSpec string(K key) {
		return new DefaultJsonStringSpec(rawKey(key), JsonPath.root(), JsonSetCondition.upsert());
	}

	@Override
	public JsonAtKeySpec value(K key) {
		return new DefaultJsonAtKeySpec(rawKey(key), JsonPath.root(), JsonSetCondition.upsert());
	}

	@Override
	public JsonResult paths(K key, Collection<String> paths) {

		Assert.notEmpty(paths, "Paths must not be empty");
		byte[] rawKey = rawKey(key);

		if (paths.stream().allMatch(BARE_PROPERTY_PATH.asMatchPredicate())) {
			return getProperties(rawKey, paths);
		}

		JsonPath[] jsonPaths = new JsonPath[paths.size()];
		int i = 0;
		for (String path : paths) {
			jsonPaths[i++] = JsonPath.raw(path);
		}

		byte[] response = execute(c -> c.jsonGet(rawKey, jsonPaths));
		return new DefaultJsonResult(this.jsonSerializer, response);
	}

	/**
	 * Read bare property names and assemble them into a single JSON object.
	 */
	@SuppressWarnings("unchecked")
	private JsonResult getProperties(byte[] rawKey, Collection<String> names) {

		JsonPath[] jsonPaths = names.stream().map(name -> JsonPath.raw("$." + name)).toArray(JsonPath[]::new);
		byte[] response = execute(c -> c.jsonGet(rawKey, jsonPaths));

		if (response == null) {
			return new DefaultJsonResult(this.jsonSerializer, null);
		}

		// JSON.GET returns a match array for one JSONPath and an object keyed by path for multiple JSONPaths.
		Map<String, List<Object>> matches;
		if (names.size() == 1) {
			String name = names.iterator().next();
			ResolvableType matchesType = ResolvableType.forClassWithGenerics(List.class, Object.class);
			List<Object> singleMatch = (List<Object>) jsonSerializer.deserialize(response, matchesType);
			matches = new LinkedHashMap<>();
			matches.put("$." + name, singleMatch);
		} else {
			ResolvableType matchesType = ResolvableType.forClassWithGenerics(Map.class, ResolvableType.forClass(String.class),
					ResolvableType.forClassWithGenerics(List.class, Object.class));
			matches = (Map<String, List<Object>>) jsonSerializer.deserialize(response, matchesType);
		}

		// best effort re-serialization to unwrap nested arrays and pull properties to the top-level.
		Map<String, Object> properties = new LinkedHashMap<>();
		for (String name : names) {
			List<Object> match = matches == null ? null : matches.get("$." + name);
			properties.put(name, match == null || match.isEmpty() ? null : match.get(0));
		}

		return new DefaultJsonResult(this.jsonSerializer, jsonSerializer.serialize(properties));
	}

	@Override
	public JsonAtKeysSpec values(Collection<K> keys) {

		Assert.notEmpty(keys, "Keys must not be empty");
		return new DefaultJsonMultiGetSpec(rawKeys(keys), JsonPath.root());
	}

	static abstract class DefaultPathSpec<P extends PathSpec<P>> implements PathSpec<P> {

		final JsonPath jsonPath;

		DefaultPathSpec(JsonPath jsonPath) {
			this.jsonPath = jsonPath;
		}

		abstract P create(JsonPath jsonPath);

		@Override
		public P root() {
			return create(JsonPath.root());
		}

		@Override
		public P path(String jsonPath) {
			return create(JsonPath.raw(jsonPath));
		}

	}

	abstract class DefaultJsonSpec<T, S extends JsonKeySupport<S> & JsonSet<T, S>> extends DefaultPathSpec<S>
			implements JsonKeySupport<S>, JsonSet<T, S> {

		final byte[] key;
		final JsonSetCondition condition;

		DefaultJsonSpec(byte[] key, JsonPath jsonPath, JsonSetCondition condition) {
			super(jsonPath);
			this.key = key;
			this.condition = condition;
		}

		abstract DefaultJsonSpec<T, S> create(byte[] key, JsonPath jsonPath, JsonSetCondition condition);

		@Override
		public @Nullable Long clear() {
			return execute(c -> c.jsonClear(key, jsonPath));
		}

		@Override
		public @Nullable Long delete() {
			return execute(c -> c.jsonDel(key, jsonPath));
		}

		@Override
		public JsonResult get() {
			byte[] result = execute(c -> c.jsonGet(key, jsonPath));
			return new DefaultJsonResult(jsonSerializer, result);
		}

		@Override
		public JsonSet<T, S> conditional(Consumer<JsonSetSpec> consumer) {

			DefaultJsonSetSpec spec = new DefaultJsonSetSpec();
			consumer.accept(spec);

			return create(key, jsonPath, spec.condition());
		}

		@Override
		public @Nullable Boolean set(T value) {
			JsonValue jsonValue = JsonValue.raw(jsonSerializer.serialize(value));
			return execute(c -> c.jsonSet(key, jsonPath, jsonValue, condition));
		}

	}

	class DefaultJsonArraySpec extends DefaultPathSpec<JsonArraySpec> implements JsonArraySpec {

		private final byte[] key;

		DefaultJsonArraySpec(byte[] key, JsonPath jsonPath) {
			super(jsonPath);
			this.key = key;
		}

		@Override
		JsonArraySpec create(JsonPath jsonPath) {
			return new DefaultJsonArraySpec(key, jsonPath);
		}

		@Override
		public List<@Nullable Long> append(Collection<? extends Object> values) {
			JsonValue[] jsonValues = values.stream().map(RedisJsonTemplate.this::serialize).toArray(JsonValue[]::new);
			return execute(c -> c.jsonArrAppend(key, jsonPath, jsonValues));
		}

		@Override
		public List<@Nullable Long> length() {
			return execute(c -> c.jsonArrLen(key, jsonPath));
		}

		@Override
		public List<@Nullable Long> trim(int start, int end) {
			return execute(c -> c.jsonArrTrim(key, jsonPath, start, end));
		}

		@Override
		public List<@Nullable Long> indexOf(Object value) {
			JsonValue jsonValue = serialize(value);
			return execute(c -> c.jsonArrIndex(key, jsonPath, jsonValue));
		}

		@Override
		public JsonArrayAtIndex index(int index) {
			return new DefaultJsonArrayAtIndex(key, jsonPath, index);
		}

	}

	class DefaultJsonArrayAtIndex implements JsonArrayAtIndex {

		private final byte[] key;
		private final JsonPath jsonPath;
		private final int index;

		DefaultJsonArrayAtIndex(byte[] key, JsonPath jsonPath, int index) {
			this.key = key;
			this.jsonPath = jsonPath;
			this.index = index;
		}

		@Override
		public List<@Nullable Long> insert(Collection<? extends Object> values) {
			JsonValue[] jsonValues = values.stream().map(RedisJsonTemplate.this::serialize).toArray(JsonValue[]::new);
			return execute(c -> c.jsonArrInsert(key, jsonPath, index, jsonValues));
		}

	}

	class DefaultJsonBooleanSpec extends DefaultJsonSpec<Boolean, JsonBooleanSpec> implements JsonBooleanSpec {

		DefaultJsonBooleanSpec(byte[] key, JsonPath jsonPath, JsonSetCondition condition) {
			super(key, jsonPath, condition);
		}

		@Override
		JsonBooleanSpec create(JsonPath jsonPath) {
			return new DefaultJsonBooleanSpec(key, jsonPath, condition);
		}

		@Override
		DefaultJsonSpec<Boolean, JsonBooleanSpec> create(byte[] key, JsonPath jsonPath, JsonSetCondition condition) {
			return new DefaultJsonBooleanSpec(key, jsonPath, condition);
		}

		@Override
		public List<@Nullable Boolean> toggle() {
			return execute(c -> c.jsonToggle(key, jsonPath));
		}

	}

	class DefaultJsonStringSpec extends DefaultJsonSpec<String, JsonStringSpec> implements JsonStringSpec {

		DefaultJsonStringSpec(byte[] key, JsonPath jsonPath, JsonSetCondition condition) {
			super(key, jsonPath, condition);
		}

		@Override
		JsonStringSpec create(JsonPath jsonPath) {
			return new DefaultJsonStringSpec(key, jsonPath, condition);
		}

		@Override
		DefaultJsonSpec<String, JsonStringSpec> create(byte[] key, JsonPath jsonPath, JsonSetCondition condition) {
			return new DefaultJsonStringSpec(key, jsonPath, condition);
		}

		@Override
		public List<@Nullable Long> length() {
			return execute(c -> c.jsonStrLen(key, jsonPath));
		}

		@Override
		public List<@Nullable Long> append(String value) {
			return execute(c -> c.jsonStrAppend(key, jsonPath, value));
		}

	}

	class DefaultJsonAtKeySpec extends DefaultJsonSpec<Object, JsonAtKeySpec> implements JsonAtKeySpec {

		DefaultJsonAtKeySpec(byte[] key, JsonPath jsonPath, JsonSetCondition condition) {
			super(key, jsonPath, condition);
		}

		@Override
		JsonAtKeySpec create(JsonPath jsonPath) {
			return new DefaultJsonAtKeySpec(key, jsonPath, condition);
		}

		@Override
		DefaultJsonSpec<Object, JsonAtKeySpec> create(byte[] key, JsonPath jsonPath, JsonSetCondition condition) {
			return new DefaultJsonAtKeySpec(key, jsonPath, condition);
		}

		@Override
		public Boolean mergeWith(Object value) {
			JsonValue jsonValue = serialize(value);
			return execute(c -> c.jsonMerge(key, jsonPath, jsonValue));
		}

		@Override
		public List<@Nullable JsonType> getType() {
			return execute(c -> c.jsonType(key, jsonPath));
		}

	}

	class DefaultJsonMultiGetSpec extends DefaultPathSpec<JsonAtKeysSpec> implements JsonAtKeysSpec {

		private final byte[][] keys;

		DefaultJsonMultiGetSpec(byte[][] keys, JsonPath jsonPath) {
			super(jsonPath);
			this.keys = keys;
		}

		@Override
		JsonAtKeysSpec create(JsonPath jsonPath) {
			return new DefaultJsonMultiGetSpec(keys, jsonPath);
		}

		@Override
		public JsonResults get() {

			List<byte[]> response = execute(c -> c.jsonMGet(jsonPath, keys));
			List<JsonResult> result = response == null ? List.of()
					: response.stream().map(it -> (JsonResult) new DefaultJsonResult(jsonSerializer, it)).toList();

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

		private static final byte[] NULL_JSON = "null".getBytes(StandardCharsets.UTF_8);

		private final RedisJsonSerializer serializer;
		private final byte @Nullable [] result;

		DefaultJsonResult(RedisJsonSerializer serializer, byte @Nullable [] result) {
			this.serializer = serializer;
			this.result = result;
		}

		@Override
		public <V> @Nullable V as(Class<V> type) {

			if (result == null) {
				return null;
			}

			if (requiresSingleElementUnwrap(type)) {
				return unwrapSingleMatch(result, ResolvableType.forClass(type));
			}
			if (type.equals(String.class)) {
				return (V) ByteUtils.toUtf8String(result);
			}
			return serializer.deserialize(result, type);
		}

		@Override
		public <V> @Nullable V as(ParameterizedTypeReference<V> type) {

			if (result == null) {
				return null;
			}

			ResolvableType resolvableType = ResolvableType.forType(type);

			if (requiresSingleElementUnwrap(resolvableType.resolve())) {
				return unwrapSingleMatch(result, resolvableType);
			}

			if (type.getType().equals(String.class)) {
				return (V) ByteUtils.toUtf8String(result);
			}
			return serializer.deserialize(result, type);
		}

		/**
		 * RedisJSON always wraps {@code JSON.GET}/{@code JSON.MGET} replies for a JSONPath query in a JSON array, one
		 * element per match, even for the root path {@code $}. A caller requesting a collection/array type wants that
		 * match-set as-is.
		 */
		// TODO: is this really a good idea? unwrapping seems neat but comes with several consequences such as the asString
		// contract
		// and while it makes object mapping pretty neat, it has some downsides.
		private <V> boolean requiresSingleElementUnwrap(@Nullable Class<V> type) {
			return !isCollectionLike(type) && looksLikeJsonArray();
		}

		private static boolean isCollectionLike(@Nullable Class<?> type) {
			return type == null || type.isArray() || Iterable.class.isAssignableFrom(type);
		}

		private boolean looksLikeJsonArray() {

			if (result == null) {
				return false;
			}

			for (byte b : result) {
				if (isJsonWhitespace(b)) {
					continue;
				}
				return b == '[';
			}

			return false;
		}

		private static boolean isJsonWhitespace(byte b) {
			return b == ' ' || b == '\t' || b == '\n' || b == '\r';
		}

		@SuppressWarnings("unchecked")
		private <V> @Nullable V unwrapSingleMatch(byte[] source, ResolvableType elementType) {

			List<V> matches = (List<V>) serializer.deserialize(source,
					ResolvableType.forClassWithGenerics(List.class, elementType));

			if (matches.isEmpty()) {
				return null;
			}

			if (matches.size() == 1) {
				V match = matches.get(0);
				if (elementType.resolve() == String.class && match != null) {
					return (V) match.toString();
				}
				return match;
			}

			throw new SerializationException("Expected exactly one JSON value but found " + matches.size());
		}

		@Override
		public <U extends @Nullable Object> U map(Function<? super byte[], ? extends U> mapper) {
			if (result == null) {
				return null;
			}
			return mapper.apply(asBytes());
		}

		@Override
		public byte[] asBytes() {
			return result == null ? NULL_JSON : result;
		}

		@Override
		public boolean isNull() {
			return result == null || Arrays.equals(NULL_JSON, result);
		}

		@Override
		public String toString() {
			return ObjectUtils.nullSafeToString(ByteUtils.toUtf8String(result));
		}

	}

	static class DefaultJsonResults implements JsonResults {

		private final Collection<JsonResult> result;

		DefaultJsonResults(Collection<JsonResult> result) {
			Assert.notNull(result, "Result must not be null");
			this.result = result;
		}

		@Override
		public <V> List<@Nullable V> as(Class<V> type) {
			return result.stream().map(it -> it.isNull() ? null : it.as(type)).toList();
		}

		@Override
		public <V> List<@Nullable V> as(ParameterizedTypeReference<V> type) {
			return result.stream().map(it -> it.isNull() ? null : it.as(type)).toList();
		}

		@Override
		public Iterator<JsonResult> iterator() {
			return result.iterator();
		}

		@Override
		public List<@Nullable String> asString() {
			List<@Nullable String> results = new ArrayList<>();
			for (JsonResult jsonResult : result) {
				results.add(jsonResult.asString());
			}
			return results;
		}

		@Override
		public List<byte @Nullable []> asBytes() {
			List<byte @Nullable []> results = new ArrayList<>();
			for (JsonResult jsonResult : result) {
				results.add(jsonResult.asBytes());
			}
			return results;
		}

		@Override
		public boolean isNull() {
			return result.isEmpty();
		}

	}

	private byte[] rawKey(K key) {

		Assert.notNull(key, "Key must not be null");
		return key instanceof byte[] bytes ? bytes : keySerializer.serialize(key);
	}

	private byte[][] rawKeys(Collection<K> keys) {

		Assert.notNull(keys, "Keys must not be null");

		byte[][] rawKeys = new byte[keys.size()][];

		int i = 0;
		for (K key : keys) {
			rawKeys[i++] = rawKey(key);
		}

		return rawKeys;
	}

	private JsonValue serialize(Object it) {
		return JsonValue.raw(jsonSerializer.serialize(it));
	}

}
