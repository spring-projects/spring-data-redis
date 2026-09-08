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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import org.jspecify.annotations.Nullable;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.data.redis.connection.json.JsonType;
import org.springframework.data.redis.serializer.RedisJsonSerializer;
import org.springframework.data.redis.serializer.SerializationException;
import org.springframework.data.redis.util.ByteUtils;
import org.springframework.data.util.Streamable;
import org.springframework.lang.CheckReturnValue;
import org.springframework.util.Assert;

/**
 * Fluent operations for storing JSON documents in Redis and accessing their values.
 * <p>
 * Use the {@link #value(Object)}, {@link #array(Object)}, {@link #string(Object)}, and {@link #bool(Object)} entry
 * points to select a key and configure an operation. Commands operate on the document root unless a JSON path is
 * selected. The following example stores a document and reads one of its properties:
 *
 * <pre class="code">
 * operations.value("user").set(new Person("John", "Doe"));
 * String firstName = operations.value("user").path("$.firstName").get().as(String.class);
 * </pre>
 * <p>
 * JSON paths follow the <a href="https://redis.io/docs/latest/develop/data-types/json/path/#jsonpath-syntax">RedisJSON
 * version 2 syntax</a>. Commands returning a {@link List} of per-path results preserve the order of the matches. Unless
 * specified otherwise, a {@literal null} element indicates an incompatible JSON type.
 * <p>
 * Operations execute when a command method such as {@link JsonKeySupport#get()} or {@link JsonSet#set(Object)} is
 * called. Key and path specifications are immutable and can be reused. Use {@link JsonResult} to access a read result
 * as JSON or deserialize it into a Java object.
 *
 * @author Mark Paluch
 * @author Yordan Tsintsov
 * @author Moritz Halbritter
 * @since 4.2
 * @param <K> the Redis key type.
 * @see RedisJsonTemplate
 */
public interface JsonOperations<K> {

	/**
	 * Start building a JSON array operation for the given {@code key}.
	 *
	 * @param key the Redis key.
	 * @return a spec for specifying the array operation.
	 */
	JsonArraySpec array(K key);

	/**
	 * Start building a JSON boolean operation for the given {@code key}.
	 *
	 * @param key the Redis key.
	 * @return a spec for specifying the boolean operation.
	 */
	JsonBooleanSpec bool(K key);

	/**
	 * Start building a JSON string operation for the given {@code key}.
	 *
	 * @param key the Redis key.
	 * @return a spec for specifying the string operation.
	 */
	JsonStringSpec string(K key);

	/**
	 * Start building a JSON value operation for the given {@code key}.
	 *
	 * @param key the Redis key.
	 * @return a spec for specifying the value operation.
	 */
	JsonAtKeySpec value(K key);

	/**
	 * Retrieve the JSON value for the given {@code key}.
	 *
	 * @param key the Redis key.
	 * @return the JSON value for the given key.
	 */
	default JsonResult get(K key) {
		return value(key).get();
	}

	/**
	 * Set the {@code key} to a JSON {@code value}.
	 *
	 * @param key the Redis key.
	 * @param value the value to write.
	 * @return {@literal true} if the value was written; {@literal false} otherwise.
	 */
	default Boolean set(K key, Object value) {
		return value(key).set(value);
	}

	/**
	 * Read the values at the given paths for a single key in one {@code JSON.GET} command.
	 * <p>
	 * See {@link #paths(Object, Collection)} for supported path forms and result mapping.
	 *
	 * @param key the Redis key.
	 * @param paths the paths to read, must not be empty or contain duplicates.
	 * @return the combined result for the requested paths.
	 * @throws IllegalArgumentException if the paths violate the requirements of {@link #paths(Object, Collection)}.
	 * @see #paths(Object, Collection)
	 */
	default JsonPathResult paths(K key, String... paths) {
		return paths(key, List.of(paths));
	}

	/**
	 * Read the values at the given paths for a single key in one {@code JSON.GET} command.
	 * <p>
	 * Paths may be property paths such as {@code "name"} and {@code "address.city"}, or JSONPath expressions such as
	 * {@code "$.name"}. All paths in a request must use the same form.
	 * <p>
	 * The result supports {@link JsonPathResult#as(Class) deserialization} into an object with one property per requested
	 * path. Property names correspond to the supplied path strings. Use {@link JsonPathResult#path(String)} to access the
	 * matches for an individual path.
	 *
	 * @param key the Redis key.
	 * @param paths the paths to read, must not be empty or contain duplicates.
	 * @return the combined result for the requested paths.
	 * @throws IllegalArgumentException if the paths are empty, contain duplicates, mix property paths with JSONPath
	 *           expressions, or include a path omitted from the Redis response.
	 * @see <a href="https://redis.io/commands/json.get">Redis Documentation: JSON.GET</a>
	 */
	JsonPathResult paths(K key, Collection<String> paths);

	/**
	 * Start building a JSON read operation for the given {@code key}.
	 *
	 * @param key the Redis key.
	 * @return a specification for selecting the path to read.
	 * @see <a href="https://redis.io/commands/json.mget">Redis Documentation: JSON.MGET</a>
	 */
	default JsonAtKeysSpec values(K key) {
		Assert.notNull(key, "Key must not be null");
		return values(List.of(key));
	}

	/**
	 * Start building a JSON read operation for the given keys.
	 *
	 * @param key the Redis key.
	 * @param additionalKeys the additional Redis keys.
	 * @return a specification for selecting the path to read.
	 * @see <a href="https://redis.io/commands/json.mget">Redis Documentation: JSON.MGET</a>
	 */
	default JsonAtKeysSpec values(K key, K... additionalKeys) {
		Assert.notNull(key, "Key must not be null");
		Assert.notNull(additionalKeys, "Additional keys must not be null");
		List<K> keys = new ArrayList<>();
		keys.add(key);
		keys.addAll(List.of(additionalKeys));
		return values(keys);
	}

	/**
	 * Start building a JSON read operation for the given keys.
	 *
	 * @param keys the Redis keys.
	 * @return a specification for selecting the path to read.
	 * @see <a href="https://redis.io/commands/json.mget">Redis Documentation: JSON.MGET</a>
	 */
	JsonAtKeysSpec values(Collection<K> keys);

	/**
	 * Specification for JSON array operations bound to a particular {@code key}.
	 * <p>
	 * All commands invoked through this interface operate on a {@link PathSpec#path(String) JSON path} defaulting to the
	 * document root ({@code $}).
	 */
	interface JsonArraySpec extends PathSpec<JsonArraySpec> {

		/**
		 * Append the given {@code values} to the JSON array at the configured path.
		 *
		 * @param values the values to append, must not be empty or {@literal null}.
		 * @return the new array length for each matching path.
		 * @see <a href="https://redis.io/commands/json.arrappend">Redis Documentation: JSON.ARRAPPEND</a>
		 */
		default List<@Nullable Long> append(Object... values) {
			return append(List.of(values));
		}

		/**
		 * Append the given {@code values} to the JSON array at the configured path.
		 *
		 * @param values the values to append, must not be empty or {@literal null}.
		 * @return the new array length for each matching path.
		 * @see <a href="https://redis.io/commands/json.arrappend">Redis Documentation: JSON.ARRAPPEND</a>
		 */
		List<@Nullable Long> append(Collection<? extends Object> values);

		/**
		 * Return the length of the JSON array at the configured path.
		 *
		 * @return the array length for each matching path.
		 * @see <a href="https://redis.io/commands/json.arrlen">Redis Documentation: JSON.ARRLEN</a>
		 */
		List<@Nullable Long> length();

		/**
		 * Trim the JSON array to the inclusive range between {@code start} and {@code end}.
		 *
		 * @param start the index of the first element to keep.
		 * @param end the index of the last element to keep. Negative values count from the end.
		 * @return the new array length for each matching path.
		 * @see <a href="https://redis.io/commands/json.arrtrim">Redis Documentation: JSON.ARRTRIM</a>
		 */
		List<@Nullable Long> trim(int start, int end);

		/**
		 * Return the first index of {@code value} within the JSON array at the configured path.
		 *
		 * @param value the value to find.
		 * @return a list where each element contains the index of the first occurrence of the value, {@code -1} if not
		 *         found, or {@literal null} if the matched value is not an array. Returns an empty list if the path does
		 *         not match any value.
		 * @see <a href="https://redis.io/commands/json.arrindex">Redis Documentation: JSON.ARRINDEX</a>
		 */
		List<@Nullable Long> indexOf(Object value);

		/**
		 * Select an array element by its {@code index} for subsequent operations.
		 *
		 * @param index the array index to operate on.
		 * @return a spec for index-based array operations.
		 */
		JsonArrayAtIndex index(int index);

	}

	/**
	 * Specification for JSON array operations bound to a previously selected array index.
	 */
	interface JsonArrayAtIndex {

		/**
		 * Insert {@code values} before the previously selected array index.
		 *
		 * @param values the values to insert, must not be empty or {@literal null}.
		 * @return the new array length for each matching path.
		 * @see <a href="https://redis.io/commands/json.arrinsert">Redis Documentation: JSON.ARRINSERT</a>
		 */
		default List<@Nullable Long> insert(Object... values) {
			return insert(List.of(values));
		}

		/**
		 * Insert {@code values} before the previously selected array index.
		 *
		 * @param values the values to insert, must not be empty or {@literal null}.
		 * @return the new array length for each matching path.
		 * @see <a href="https://redis.io/commands/json.arrinsert">Redis Documentation: JSON.ARRINSERT</a>
		 */
		List<@Nullable Long> insert(Collection<? extends Object> values);

	}

	/**
	 * Specification for JSON boolean operations bound to a particular {@code key}.
	 *
	 * @see <a href="https://redis.io/commands/json.toggle">Redis Documentation: JSON.TOGGLE</a>
	 */
	interface JsonBooleanSpec extends JsonKeySupport<JsonBooleanSpec>, JsonSet<Boolean, JsonBooleanSpec> {

		/**
		 * Toggle the boolean values at the configured path.
		 *
		 * @return the updated value for each matching path, with a {@literal null} element for each non-boolean value.
		 * @see <a href="https://redis.io/commands/json.toggle">Redis Documentation: JSON.TOGGLE</a>
		 */
		List<@Nullable Boolean> toggle();

	}

	/**
	 * Specification for JSON string operations bound to a particular {@code key}.
	 */
	interface JsonStringSpec extends JsonKeySupport<JsonStringSpec>, JsonSet<String, JsonStringSpec> {

		/**
		 * Return the length of the JSON string values at the configured path.
		 *
		 * @return the length for each matching path, with a {@literal null} element for each non-string value.
		 * @see <a href="https://redis.io/commands/json.strlen">Redis Documentation: JSON.STRLEN</a>
		 */
		List<@Nullable Long> length();

		/**
		 * Append {@code value} to the JSON string values at the configured path.
		 *
		 * @param value the string value to append.
		 * @return the updated length for each matching path, with a {@literal null} element for each non-string value.
		 * @see <a href="https://redis.io/commands/json.strappend">Redis Documentation: JSON.STRAPPEND</a>
		 */
		List<@Nullable Long> append(String value);

	}

	/**
	 * Specification for JSON value operations bound to a particular {@code key}. Provides access to type-agnostic
	 * operations such as {@link #mergeWith(Object) merge} and {@link #getType() type} inspection.
	 */
	interface JsonAtKeySpec extends JsonKeySupport<JsonAtKeySpec>, JsonSet<Object, JsonAtKeySpec> {

		/**
		 * Merge {@code value} into the JSON value at the configured path.
		 *
		 * @param value the value to write.
		 * @return {@literal true} if the merge was applied; {@literal false} otherwise.
		 * @see <a href="https://redis.io/commands/json.merge">Redis Documentation: JSON.MERGE</a>
		 */
		Boolean mergeWith(Object value);

		/**
		 * Determine the {@link JsonType type} of the JSON values at the configured path.
		 *
		 * @return a list containing the JSON types for matching paths.
		 * @see <a href="https://redis.io/commands/json.type">Redis Documentation: JSON.TYPE</a>
		 */
		List<@Nullable JsonType> getType();

	}

	/**
	 * Specification for JSON multi-key operations sharing a common JSON path.
	 *
	 * @see <a href="https://redis.io/commands/json.mget">Redis Documentation: JSON.MGET</a>
	 */
	interface JsonAtKeysSpec extends PathSpec<JsonAtKeysSpec>, JsonMultiGetSpec {

	}

	/**
	 * Common support for JSON operations bound to a single key and configurable path.
	 *
	 * @param <P> self-type used for fluent method chaining.
	 */
	interface JsonKeySupport<P extends JsonKeySupport<P>> extends PathSpec<P> {

		/**
		 * Clear the JSON values at the configured path.
		 *
		 * @return the number of values that were cleared.
		 * @see <a href="https://redis.io/commands/json.clear">Redis Documentation: JSON.CLEAR</a>
		 */
		Long clear();

		/**
		 * Delete the JSON values at the configured path.
		 *
		 * @return the number of values that were deleted.
		 * @see <a href="https://redis.io/commands/json.del">Redis Documentation: JSON.DEL</a>
		 */
		Long delete();

		/**
		 * Retrieve the JSON value at the configured path.
		 *
		 * @return the result containing the values matched by the configured path.
		 * @see <a href="https://redis.io/commands/json.get">Redis Documentation: JSON.GET</a>
		 */
		JsonResult get();

	}

	/**
	 * Common support for setting JSON values at the currently configured path.
	 *
	 * @param <T> value type.
	 * @param <S> self-type used for fluent method chaining.
	 * @see <a href="https://redis.io/commands/json.set">Redis Documentation: JSON.SET</a>
	 */
	interface JsonSet<T, S extends JsonSet<T, S>> {

		/**
		 * Apply a condition to the set operation through a {@link JsonSetSpec}.
		 *
		 * @param consumer callback to configure the condition.
		 * @return a new spec instance.
		 */
		@CheckReturnValue
		JsonSet<T, S> conditional(Consumer<JsonSetSpec> consumer);

		/**
		 * Set the JSON {@code value} at the configured path.
		 *
		 * @param value the value to write.
		 * @return {@literal true} if the value was written; {@literal false} otherwise.
		 * @see <a href="https://redis.io/commands/json.set">Redis Documentation: JSON.SET</a>
		 */
		Boolean set(T value);

		/**
		 * Set the JSON {@code value} at the configured path only if the path has one or more matches ({@code XX}).
		 *
		 * @param value the value to write.
		 * @return {@literal true} if the value was written; {@literal false} otherwise.
		 */
		default Boolean setIfPresent(T value) {
			return conditional(JsonSetSpec::ifPresent).set(value);
		}

		/**
		 * Set the JSON {@code value} at the configured path only if the path has no matches ({@code NX}).
		 *
		 * @param value the value to write.
		 * @return {@literal true} if the value was written; {@literal false} otherwise.
		 */
		default Boolean setIfAbsent(T value) {
			return conditional(JsonSetSpec::ifAbsent).set(value);
		}

	}

	/**
	 * Terminal step for executing a JSON read operation across multiple keys.
	 */
	interface JsonMultiGetSpec {

		/**
		 * Execute the read operation and return the resulting JSON values.
		 *
		 * @return the JSON values.
		 */
		JsonResults get();
	}

	/**
	 * Common support for selecting the JSON path against which commands operate.
	 *
	 * @param <P> self-type used for fluent method chaining.
	 */
	interface PathSpec<P extends PathSpec<P>> {

		/**
		 * Select the document root path ({@code $}).
		 *
		 * @return a new spec instance.
		 */
		@CheckReturnValue
		P root();

		/**
		 * Select the JSON path to operate on.
		 *
		 * @param jsonPath the JSONPath expression.
		 * @return a new spec instance.
		 */
		@CheckReturnValue
		P path(String jsonPath);

	}

	/**
	 * The result of a JSON command, with access to its JSON representation and deserialized value.
	 * <p>
	 * A JSONPath query returns a match array containing zero or more values, including when the path selects the document
	 * root. Use {@link #as(Class)} to deserialize a single match or {@link #matches()} to access individual matches. A
	 * matched value can itself be an object, an array, or a scalar.
	 * <p>
	 * Results obtained from {@code matches()} represent individual values without a surrounding match array. The
	 * {@link #asBytes()} and {@link #asString()} methods preserve any match array present in this result.
	 */
	interface JsonResult {

		/**
		 * Deserialize this result into an object of the given type.
		 * <p>
		 * If this result contains a match array, deserialize its single element. Use {@link #matches()} to access multiple
		 * matches.
		 *
		 * @param type the target type.
		 * @param <V> the result type.
		 * @return the deserialized value, or {@literal null} if the key is absent, the path has no matches, or the value is
		 *         JSON {@literal null}.
		 * @throws SerializationException if there is more than one match or the value cannot be deserialized.
		 */
		<V> @Nullable V as(Class<V> type);

		/**
		 * Deserialize this result into an object of the given type.
		 * <p>
		 * If this result contains a match array, deserialize its single element. Use {@link #matches()} to access multiple
		 * matches.
		 *
		 * @param type the target type.
		 * @param <V> the result type.
		 * @return the deserialized value, or {@literal null} if the key is absent, the path has no matches, or the value is
		 *         JSON {@literal null}.
		 * @throws SerializationException if there is more than one match or the value cannot be deserialized.
		 */
		<V> @Nullable V as(ParameterizedTypeReference<V> type);

		/**
		 * Return the individual matches in this result.
		 * <p>
		 * Each element of a match array becomes a separate {@code JsonResult}. For a result that already represents an
		 * individual value, return a sequence containing this result.
		 *
		 * @return the matches in order, or an empty sequence if the key is absent or the path has no matches.
		 */
		JsonResults matches();

		/**
		 * Return the JSON representation of this result as raw bytes.
		 * <p>
		 * The match array, if present, is retained. A result obtained directly from Redis preserves its original
		 * representation. For values extracted from a larger response, preservation depends on the configured
		 * {@link RedisJsonSerializer}. The built-in Jackson serializers preserve the original bytes.
		 *
		 * @return the JSON bytes, or {@literal null} if the key does not exist.
		 * @see RedisJsonSerializer#splitArray(byte[])
		 * @see RedisJsonSerializer#splitObject(byte[])
		 */
		byte @Nullable [] asBytes();

		/**
		 * Return the JSON representation of this result as a UTF-8 string.
		 * <p>
		 * The match array, if present, is retained.
		 *
		 * @return the JSON string, or {@literal null} if the key does not exist.
		 * @see #asBytes()
		 */
		default @Nullable String asString() {
			return ByteUtils.toUtf8String(asBytes());
		}

		/**
		 * Apply the given function to the raw JSON bytes of this result.
		 * <p>
		 * The function receives the same representation as {@link #asBytes()}, including any match array or JSON
		 * {@literal null} value. It is not invoked if the key does not exist.
		 *
		 * @param mapper the function to apply to the JSON bytes.
		 * @param <U> the mapped result type.
		 * @return the function result, or {@literal null} if the key does not exist.
		 */
		default <U extends @Nullable Object> U map(Function<? super byte[], ? extends U> mapper) {

			byte[] bytes = asBytes();
			return bytes == null ? null : mapper.apply(bytes);
		}

		/**
		 * Return whether this result represents JSON {@literal null}.
		 * <p>
		 * For a match array, this method returns {@literal true} only if the array contains a single JSON {@literal null}
		 * value. It returns {@literal false} if the key is absent or the path has no matches.
		 *
		 * @return {@literal true} if this result represents JSON {@literal null}; {@literal false} otherwise.
		 * @see #exists()
		 * @see #matches()
		 */
		boolean isNull();

		/**
		 * Return whether the key exists.
		 * <p>
		 * An existing key may have no matches for the selected path or contain a JSON {@literal null} value.
		 *
		 * @return {@literal true} if the key exists; {@literal false} otherwise.
		 * @see #matches()
		 * @see #isNull()
		 */
		boolean exists();

	}

	/**
	 * A sequence of JSON results with access to their JSON representations and deserialized values.
	 * <p>
	 * Elements retain the order of the matching paths or input keys of the originating command.
	 */
	interface JsonResults extends Streamable<JsonResult> {

		/**
		 * Deserialize each JSON result into an object of the given type.
		 *
		 * @param type the target type for each result.
		 * @return the deserialized values in result order.
		 * @param <V> the result element type.
		 */
		<V> List<@Nullable V> as(Class<V> type);

		/**
		 * Deserialize each JSON result into an object of the given type.
		 *
		 * @param type the target type for each result.
		 * @return the deserialized values in result order.
		 * @param <V> the result element type.
		 */
		<V> List<@Nullable V> as(ParameterizedTypeReference<V> type);

		/**
		 * Return the JSON representation of each result as a UTF-8 string.
		 *
		 * @return the JSON string representations.
		 */
		List<@Nullable String> asString();

		/**
		 * Return the JSON representation of each result as raw bytes.
		 *
		 * @return the raw JSON bytes.
		 */
		List<byte @Nullable []> asBytes();

	}

	/**
	 * The combined result of reading several paths from a single JSON document.
	 * <p>
	 * The {@link #as(Class)} methods deserialize an object with one property per requested path. Property names
	 * correspond to the path strings supplied to {@link JsonOperations#paths(Object, Collection)}. Each property's value
	 * is extracted from that path's match array. A path with no matches contributes JSON {@literal null}. Use
	 * {@link #path(String)} to access the matches for an individual path.
	 * <p>
	 * The raw {@code JSON.GET} response contains a match array for a single requested path. For multiple paths, it
	 * contains an object keyed by the JSONPath expressions sent to Redis. Property paths are converted to bracket
	 * notation, so a request for {@code "name"} uses {@code "$['name']"} as the response key.
	 *
	 * @since 4.2
	 */
	interface JsonPathResult {

		/**
		 * Return the {@code JSON.GET} response as raw bytes.
		 * <p>
		 * The response retains the match arrays and any JSONPath keys returned by Redis.
		 *
		 * @return the JSON bytes, or {@literal null} if the key does not exist.
		 */
		byte @Nullable [] asBytes();

		/**
		 * Return the {@code JSON.GET} response as a UTF-8 string.
		 * <p>
		 * The response retains the match arrays and any JSONPath keys returned by Redis.
		 *
		 * @return the JSON string, or {@literal null} if the key does not exist.
		 */
		default @Nullable String asString() {
			return ByteUtils.toUtf8String(asBytes());
		}

		/**
		 * Apply the given function to the raw {@code JSON.GET} response.
		 * <p>
		 * The function receives the same representation as {@link #asBytes()}, including the match arrays and any JSONPath
		 * keys. It is not invoked if the key does not exist.
		 *
		 * @param mapper the function to apply to the response bytes.
		 * @param <U> the mapped result type.
		 * @return the function result, or {@literal null} if the key does not exist.
		 */
		default <U extends @Nullable Object> U map(Function<? super byte[], ? extends U> mapper) {

			byte[] bytes = asBytes();
			return bytes == null ? null : mapper.apply(bytes);
		}

		/**
		 * Deserialize the selected values into an object of the given type.
		 * <p>
		 * Each requested path contributes one property as described in {@link JsonPathResult}. Use {@link #path(String)} to
		 * access a path with multiple matches.
		 *
		 * @param type the target type.
		 * @param <V> the result type.
		 * @return the deserialized object, or {@literal null} if the key does not exist.
		 * @throws SerializationException if a path has more than one match or the object cannot be deserialized.
		 */
		<V> @Nullable V as(Class<V> type);

		/**
		 * Deserialize the selected values into an object of the given type.
		 * <p>
		 * Each requested path contributes one property as described in {@link JsonPathResult}. Use {@link #path(String)} to
		 * access a path with multiple matches.
		 *
		 * @param type the target type.
		 * @param <V> the result type.
		 * @return the deserialized object, or {@literal null} if the key does not exist.
		 * @throws SerializationException if a path has more than one match or the object cannot be deserialized.
		 */
		<V> @Nullable V as(ParameterizedTypeReference<V> type);

		/**
		 * Return the result for a requested path.
		 * <p>
		 * The result contains the path's match array and supports any number of matches.
		 *
		 * @param path the path string supplied to {@link JsonOperations#paths(Object, Collection)}, must not be
		 *          {@literal null}.
		 * @return the result for the requested path.
		 * @throws IllegalArgumentException if the path was not requested.
		 */
		JsonResult path(String path);

		/**
		 * Return whether the key exists.
		 * <p>
		 * An existing key may have no matches for individual paths.
		 *
		 * @return {@literal true} if the key exists; {@literal false} otherwise.
		 */
		boolean exists();

	}

	/**
	 * Mutable configuration of the condition for a {@code JSON.SET} operation.
	 *
	 * @see <a href="https://redis.io/commands/json.set">Redis Documentation: JSON.SET</a>
	 */
	interface JsonSetSpec {

		/**
		 * Configure the operation to set the value unconditionally.
		 *
		 * @return this builder.
		 */
		JsonSetSpec always();

		/**
		 * Configure the operation to set the value only if the target path does not exist ({@code NX}).
		 *
		 * @return this builder.
		 */
		JsonSetSpec ifAbsent();

		/**
		 * Configure the operation to set the value only if the target path exists ({@code XX}).
		 *
		 * @return this builder.
		 */
		JsonSetSpec ifPresent();

	}

}
