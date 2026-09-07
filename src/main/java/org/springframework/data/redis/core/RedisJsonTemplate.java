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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;

import org.jspecify.annotations.Nullable;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.ResolvableType;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisJsonCommands;
import org.springframework.data.redis.connection.RedisKeyCommands;
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
 * <p>
 * Person person = operations.value("key").get().as(Person.class);
 * </pre>
 * <p>
 * JSON path expressions follow the
 * <a href="https://redis.io/docs/latest/develop/data-types/json/path/#jsonpath-syntax">RedisJSON</a> version 2 path
 * syntax. Unless specified otherwise, results are positionally correlated to matching paths: each element in the
 * returned {@link List} corresponds to a matching path, with {@literal null} indicating that the matched value has an
 * incompatible JSON type.
 * <p>
 * Specification objects are immutable and can be used to build up complex queries.
 *
 * @author Yordan Tsintsov
 * @author Mark Paluch
 * @author Moritz Halbritter
 * @since 4.2
 * @param <K> the Redis key type.
 * @see RedisJsonOperations
 * @see JsonPath
 */
public class RedisJsonTemplate<K> implements RedisJsonOperations<K> {

	/**
	 * Matches a bare property path such as {@code name} or {@code address.city} and rejects double dots.
	 */
	private static final Pattern BARE_PROPERTY_PATH = Pattern.compile("[\\w-]+(?:\\.[\\w-]+)*",
			Pattern.UNICODE_CHARACTER_CLASS);

	private static final byte[] JSON_NULL = "null".getBytes(StandardCharsets.UTF_8);

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

	@SuppressWarnings("removal")
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
	 * @see RedisJsonTemplate(RedisConnectionFactory, RedisSerializer, RedisJsonSerializer)
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
	 * @see RedisJsonTemplate(RedisConnectionFactory, RedisSerializer, RedisJsonSerializer)
	 */
	public static RedisJsonTemplate<String> create(RedisConnectionFactory connectionFactory,
			RedisJsonSerializer jsonSerializer) {
		return new RedisJsonTemplate<>(connectionFactory, RedisSerializer.string(), jsonSerializer);
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
	public JsonPathResult paths(K key, Collection<String> paths) {

		Assert.notEmpty(paths, "Paths must not be empty");

		if (new HashSet<>(paths).size() != paths.size()) {
			throw new IllegalArgumentException("Duplicate paths are not supported, got: " + paths);
		}

		byte[] rawKey = rawKey(key);

		long bare = paths.stream().filter(BARE_PROPERTY_PATH.asMatchPredicate()).count();
		if (bare > 0 && bare != paths.size()) {
			throw new IllegalArgumentException("Mixing bare property names and JSONPath expressions is not supported");
		}

		boolean isBare = bare == paths.size();

		List<RequestedPath> requestedPaths = new ArrayList<>(paths.size());
		JsonPath[] jsonPaths = new JsonPath[paths.size()];

		int i = 0;
		for (String path : paths) {
			String sent = isBare ? toBracketPath(path) : path;
			requestedPaths.add(new RequestedPath(path, sent));
			jsonPaths[i++] = JsonPath.raw(sent);
		}

		byte[] response = execute(c -> c.jsonGet(rawKey, jsonPaths));
		return new DefaultJsonPathResult(this.jsonSerializer, requestedPaths, response);
	}

	/**
	 * Turn a bare property path such as {@code address.city} into the bracket-notation JSONPath
	 * {@code $['address']['city']}.
	 * <p>
	 * Dot notation cannot be used here: RedisJSON's JSONPath parser only accepts ASCII identifiers after a {@code .},
	 * so a property name such as {@code "äx"} - which {@link #BARE_PROPERTY_PATH} accepts, since {@code \w} is
	 * Unicode-aware here - would make {@code $.äx} fail. Worse, RedisJSON reports that failure differently depending
	 * on the path count: a single bad path errors, while a bad path alongside good ones is silently omitted from the
	 * reply, which would surface as the property having matched nothing.
	 * <p>
	 * A literal replace of the separator is enough: {@link #BARE_PROPERTY_PATH} admits only word characters,
	 * {@code -} and the {@code .} separator, so no segment can contain a quote, backslash or bracket that would need
	 * escaping.
	 */
	private static String toBracketPath(String barePath) {
		return "$['" + barePath.replace(".", "']['") + "']";
	}

	@Override
	public JsonAtKeysSpec values(Collection<K> keys) {

		Assert.notEmpty(keys, "Keys must not be empty");
		return new DefaultJsonMultiGetSpec(rawKeys(keys), JsonPath.root());
	}

	@Override
	public KeySpec key(K key) {

		Assert.notNull(key, "Key must not be null");
		return new DefaultKeySpec(rawKey(key));
	}

	@Override
	public KeysSpec keys(Collection<K> keys) {

		Assert.notEmpty(keys, "Keys must not be empty");
		return new DefaultKeysSpec(rawKeys(keys));
	}

	@Override
	public Boolean delete(K key) {

		byte[] rawKey = rawKey(key);

		Long result = doWithKeys(connection -> connection.del(rawKey));
		return result != null && result.intValue() == 1;
	}

	private @Nullable <T> T doWithKeys(Function<RedisKeyCommands, T> action) {

		RedisConnection connection = RedisConnectionUtils.getConnection(connectionFactory);

		try {
			return action.apply(connection.keyCommands());
		} finally {
			RedisConnectionUtils.releaseConnection(connection, connectionFactory);
		}
	}

	private <T extends @Nullable Object> T execute(Function<RedisJsonCommands, T> action) {

		RedisConnection connection = RedisConnectionUtils.getConnection(connectionFactory);

		try {
			return action.apply(connection.jsonCommands());
		} finally {
			RedisConnectionUtils.releaseConnection(connection, connectionFactory);
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
			return DefaultJsonResult.ofMatchArray(jsonSerializer, result);
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
					: response.stream().map(it -> (JsonResult) DefaultJsonResult.ofMatchArray(jsonSerializer, it)).toList();

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
		private final byte @Nullable [] result;
		private final boolean matchArray;

		/**
		 * Lazily split elements of {@link #result}, cached because {@link #matches()} and {@link #isNull()} both need
		 * them and splitting is not cheap - {@link RedisJsonSerializer#splitArray} may round-trip the whole
		 * match array through the serializer.
		 */
		private @Nullable List<byte[]> elements;

		/**
		 * @param matchArray whether {@code result} is a RedisJSON JSONPath match array, one element per match.
		 */
		private DefaultJsonResult(RedisJsonSerializer serializer, byte @Nullable [] result, boolean matchArray) {
			this.serializer = serializer;
			this.result = result;
			this.matchArray = matchArray;
		}

		@Override
		public <V> @Nullable V as(Class<V> type) {
			return as(ResolvableType.forClass(type));
		}

		@Override
		public <V> @Nullable V as(ParameterizedTypeReference<V> type) {
			return as(ResolvableType.forType(type));
		}

		@SuppressWarnings("unchecked")
		private <V> @Nullable V as(ResolvableType type) {

			if (result == null) {
				return null;
			}

			if (matchArray) {
				return unwrapSingleMatch(result, type);
			}

			return (V) serializer.deserialize(result, type);
		}

		@SuppressWarnings("unchecked")
		private <V> @Nullable V unwrapSingleMatch(byte[] source, ResolvableType elementType) {

			List<V> matches = (List<V>) serializer.deserialize(source,
					ResolvableType.forClassWithGenerics(List.class, elementType));

			if (matches == null || matches.isEmpty()) {
				return null;
			}

			if (matches.size() == 1) {
				return matches.get(0);
			}

			throw new SerializationException(
					"Expected exactly one JSON value but found " + matches.size() + ", use matches() to read them all");
		}

		@Override
		public JsonResults matches() {

			if (!matchArray) {
				return new DefaultJsonResults(List.of(this));
			}

			if (result == null) {
				return new DefaultJsonResults(List.of());
			}

			List<JsonResult> children = elements().stream().map(element -> (JsonResult) ofValue(serializer, element))
					.toList();
			return new DefaultJsonResults(children);
		}

		@Override
		public byte @Nullable [] asBytes() {
			return result;
		}

		@Override
		public boolean isNull() {

			if (result == null) {
				return false;
			}

			if (!matchArray) {
				return Arrays.equals(result, JSON_NULL);
			}

			List<byte[]> elements = elements();
			return elements.size() == 1 && Arrays.equals(elements.get(0), JSON_NULL);
		}

		private List<byte[]> elements() {

			List<byte[]> elements = this.elements;

			if (elements == null) {
				elements = List.copyOf(serializer.splitArray(Objects.requireNonNull(result)));
				this.elements = elements;
			}

			return elements;
		}

		@Override
		public boolean exists() {
			return result != null;
		}

		@Override
		public String toString() {
			return ObjectUtils.nullSafeToString(ByteUtils.toUtf8String(result));
		}

		/**
		 * A payload taken straight off the wire: a match array, one element per match, or {@literal null} for an absent
		 * key.
		 */
		static DefaultJsonResult ofMatchArray(RedisJsonSerializer serializer, byte @Nullable [] result) {
			return new DefaultJsonResult(serializer, result, true);
		}

		/**
		 * A single already-unwrapped match, decoded as-is. Used by {@link #matches()} to wrap the elements it splits out
		 * of a match array.
		 */
		static DefaultJsonResult ofValue(RedisJsonSerializer serializer, byte[] value) {

			Assert.notNull(value, "Value must not be null");
			return new DefaultJsonResult(serializer, value, false);
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
			return result.stream().map(it -> it.as(type)).toList();
		}

		@Override
		public <V> List<@Nullable V> as(ParameterizedTypeReference<V> type) {
			return result.stream().map(it -> it.as(type)).toList();
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

	}

	/**
	 * A single requested path for {@link #paths(Object, Collection)}, pairing the caller's string with the JSONPath
	 * actually sent to Redis. These differ for bare property paths, where {@code "name"} is sent as
	 * {@code "$['name']"} - see {@link #toBracketPath}.
	 */
	record RequestedPath(String requested, String sent) {
	}

	/**
	 * Implements {@link JsonPathResult} on top of a single {@code JSON.GET key $.path1 $.path2 ...} reply.
	 * <p>
	 * Redis replies with an object keyed by the sent JSONPaths, one match array per path, e.g.
	 * {@code {"$['a']":[1],"$['b']":[2]}} for {@code paths(key, List.of("a", "b"))} - or, for a single requested path,
	 * with a bare match array. {@link #parseMembers} eagerly reduces both shapes to {@link #members}, keyed by the
	 * caller's requested name, which is what {@link #path(String)} looks up. {@link #flatten(Map)} then unwraps those
	 * match arrays into the object {@link #as} decodes, e.g. {@code {"a":1,"b":2}}; {@link #asBytes()} and
	 * {@link #exists()} answer from the raw {@link #reply} instead.
	 */
	static class DefaultJsonPathResult implements JsonPathResult {

		private final RedisJsonSerializer serializer;
		private final List<RequestedPath> requestedPaths;
		private final byte @Nullable [] reply;
		private final @Nullable Map<String, byte[]> members;

		/**
		 * Lazily built flattened object, cached because both {@link #as} overloads need it and building it round-trips
		 * every member through {@link RedisJsonSerializer#joinObject}.
		 */
		private volatile byte @Nullable [] flattened;

		DefaultJsonPathResult(RedisJsonSerializer serializer, List<RequestedPath> requestedPaths,
				byte @Nullable [] reply) {

			this.serializer = serializer;
			this.requestedPaths = requestedPaths;
			this.reply = reply;
			this.members = reply == null ? null : parseMembers(serializer, requestedPaths, reply);
		}

		/**
		 * Reduce the raw {@code reply} to one match array per requested path, keyed by
		 * {@link RequestedPath#requested()}. A single requested path replies with a bare match array, which is stored
		 * as-is and therefore always holds the bytes Redis sent; several paths reply with an object keyed by
		 * {@link RequestedPath#sent()}, whose members are re-keyed here and are as exact as
		 * {@link RedisJsonSerializer#splitObject} is - see {@link JsonResult#asBytes()}.
		 * <p>
		 * Redis echoes back every path it accepted, using an empty match array for one that matched nothing, so a
		 * path absent from the reply is one its JSONPath parser rejected. Redis reports that rejection as an error
		 * only when it is the sole path; alongside accepted paths it just drops the member. Rejecting it here keeps
		 * that from being read as "matched nothing".
		 */
		private static Map<String, byte[]> parseMembers(RedisJsonSerializer serializer,
				List<RequestedPath> requestedPaths, byte[] reply) {

			Map<String, byte[]> members = new LinkedHashMap<>();

			if (requestedPaths.size() == 1) {
				members.put(requestedPaths.get(0).requested(), reply);
				return members;
			}

			Map<String, byte[]> bySentPath = serializer.splitObject(reply);

			for (RequestedPath requestedPath : requestedPaths) {

				byte[] matchArray = bySentPath.get(requestedPath.sent());

				if (matchArray == null) {
					throw new IllegalArgumentException("Redis did not return path '%s'. '%s' is not a valid JSONPath expression".formatted(requestedPath.requested(), requestedPath.requested()));
				}

				members.put(requestedPath.requested(), matchArray);
			}

			return members;
		}

		@Override
		public byte @Nullable [] asBytes() {
			return reply;
		}

		@Override
		public <V> @Nullable V as(Class<V> type) {
			return members == null ? null : serializer.deserialize(flatten(members), type);
		}

		@Override
		public <V> @Nullable V as(ParameterizedTypeReference<V> type) {
			return members == null ? null : serializer.deserialize(flatten(members), type);
		}

		/**
		 * Return the flattened object {@link #as} decodes, building it on first use. See {@link #flattened}.
		 */
		private byte[] flatten(Map<String, byte[]> members) {

			byte[] flattened = this.flattened;

			if (flattened == null) {
				flattened = buildFlattened(members);
				this.flattened = flattened;
			}

			return flattened;
		}

		/**
		 * Build the flattened object {@link #as} decodes, e.g. {@code {"a":1,"b":2}}, by unwrapping each match array in
		 * {@code members} via {@link #extractSingleElement}.
		 */
		private byte[] buildFlattened(Map<String, byte[]> members) {

			Map<String, byte[]> flattened = new LinkedHashMap<>();

			for (RequestedPath requestedPath : requestedPaths) {

				byte[] matchArray = members.get(requestedPath.requested());
				flattened.put(requestedPath.requested(), extractSingleElement(requestedPath.requested(), matchArray));
			}

			return serializer.joinObject(flattened);
		}

		/**
		 * Unwrap one path's match array (e.g. {@code ["1"]}) into the plain value {@code flatten} embeds (e.g.
		 * {@code 1}). An empty match array (the path matched nothing) becomes the JSON literal {@code null}; more than
		 * one match is rejected, since the flattened object holds one value per path.
		 */
		private byte[] extractSingleElement(String path, byte[] matchArray) {

			List<byte[]> elements = serializer.splitArray(matchArray);

			if (elements.isEmpty()) {
				return "null".getBytes(StandardCharsets.UTF_8);
			}

			if (elements.size() == 1) {
				return elements.get(0);
			}

			throw new SerializationException(
					"Path '" + path + "' matched more than once, use path(String) to read its match array");
		}

		@Override
		public JsonResult path(String path) {

			if (requestedPaths.stream().noneMatch(it -> it.requested().equals(path))) {
				throw new IllegalArgumentException("Path '" + path + "' was not requested");
			}

			return DefaultJsonResult.ofMatchArray(serializer, members == null ? null : members.get(path));
		}

		@Override
		public boolean exists() {
			return reply != null;
		}

	}

	class DefaultKeySpec implements KeySpec {

		private final byte[] key;

		DefaultKeySpec(byte[] key) {
			this.key = key;
		}

		@Override
		public Boolean delete() {
			return doWithKeys(it -> it.del(key)) == 1;
		}

		@Override
		public Boolean unlink() {
			return doWithKeys(it -> it.unlink(key)) == 1;
		}
	}

	class DefaultKeysSpec implements KeysSpec {

		private final byte[][] keys;

		DefaultKeysSpec(byte[][] keys) {
			this.keys = keys;
		}

		@Override
		public Long delete() {
			return doWithKeys(it -> it.del(keys));
		}

		@Override
		public Long unlink() {
			return doWithKeys(it -> it.unlink(keys));
		}

	}

}
