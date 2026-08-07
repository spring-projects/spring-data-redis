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
import org.springframework.data.redis.connection.json.JsonValue;
import org.springframework.data.redis.serializer.SerializationException;
import org.springframework.data.util.Streamable;
import org.springframework.lang.CheckReturnValue;
import org.springframework.util.Assert;

/**
 * Redis operations for JSON values providing a fluent API for flexible JSON result consumption.
 * <p>
 * Typed entry points bound to a key allow configuring the command and running it by calling a terminal method returning
 * the command result, for example:
 *
 * <pre class="code">
 * operations.value("key").set("value");
 * operations.value("key").path("$..name").setIfAbsent("Doe");
 * operations.array("key").path("$.names").index(2).insert("John")
 * Person person = operations.value("key").get().as(Person.class);
 * </pre>
 * <p>
 * JSON path expressions follow the
 * <a href="https://redis.io/docs/latest/develop/data-types/json/path/#jsonpath-syntax">RedisJSON</a> path syntax.
 * Unless specified otherwise, results are positionally correlated to matching paths: each element in the returned
 * {@link List} corresponds to a matching path, with {@literal null} indicating that the matched value has an
 * incompatible JSON type.
 * <p>
 * Specification objects are immutable and can be used to build up complex queries.
 *
 * @author Mark Paluch
 * @author Yordan Tsintsov
 * @since 4.2
 * @param <K> the Redis key type.
 */
public interface JsonOperations<K> {

	/**
	 * Start building a JSON array operation for the given {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return a spec for specifying the array operation.
	 */
	JsonArraySpec array(K key);

	/**
	 * Start building a JSON boolean operation for the given {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return a spec for specifying the boolean operation.
	 */
	JsonBooleanSpec bool(K key);

	/**
	 * Start building a JSON string operation for the given {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return a spec for specifying the string operation.
	 */
	JsonStringSpec string(K key);

	/**
	 * Start building a JSON value operation for the given {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return a spec for specifying the value operation.
	 */
	JsonAtKeySpec value(K key);

	/**
	 * Retrieve the JSON value for the given {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return the JSON value for the given key.
	 */
	default JsonResult get(K key) {
		return value(key).get();
	}

	/**
	 * Set the {@code key} to a JSON {@code value}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return {@literal true} if the value was written; {@literal false} otherwise.
	 */
	default Boolean set(K key, Object value) {
		return value(key).set(value);
	}

	/**
	 * Read the JSON values at the given {@code paths} for the given {@code key} in a single {@code JSON.GET}. The
	 * supplied paths form the complete set to retrieve and are returned as one combined JSON document.
	 * <p>
	 * If every supplied path is a bare property path consisting (for example, {@code "name"} or {@code "address.city"}),
	 * without JSONPath syntax such as * {@code $ [ ] ( ) ? @ ~}, the result is instead assembled into a single JSON
	 * object keyed by the given property names (e.g. {@code {"name": "John Doe", "age": 34}}), with {@literal null} for a
	 * property whose path did not match. Mixing bare property paths with JSONPath expressions in the same call disables
	 * this and falls back to the combined-document form above.
	 *
	 * @param key must not be {@literal null}.
	 * @param paths the JSON paths to read, must not be empty.
	 * @return the combined JSON document containing the requested paths.
	 * @see <a href="https://redis.io/commands/json.get">Redis Documentation: JSON.GET</a>
	 */
	default JsonResult paths(K key, String... paths) {
		return paths(key, List.of(paths));
	}

	/**
	 * Read the JSON values at the given {@code paths} for the given {@code key} in a single {@code JSON.GET}. The
	 * supplied paths form the complete set to retrieve and are returned as one combined JSON document.
	 * <p>
	 * If every supplied path is a bare property path consisting (for example, {@code "name"} or {@code "address.city"}),
	 * without JSONPath syntax such as * {@code $ [ ] ( ) ? @ ~}, the result is instead assembled into a single JSON
	 * object keyed by the given property names (e.g. {@code {"name": "John Doe", "age": 34}}), with {@literal null} for a
	 * property whose path did not match. Mixing bare property paths with JSONPath expressions in the same call disables
	 * this and falls back to the combined-document form above.
	 *
	 * @param key must not be {@literal null}.
	 * @param paths the JSON paths to read; must not be empty.
	 * @return the combined JSON document containing the requested paths.
	 * @see <a href="https://redis.io/commands/json.get">Redis Documentation: JSON.GET</a>
	 */
	JsonResult paths(K key, Collection<String> paths);

	/**
	 * Start building a JSON multi-value operation (i.e. multi-get) for the given {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return a spec for specifying the multi-value operation.
	 * @see <a href="https://redis.io/commands/json.mget">Redis Documentation: JSON.MGET</a>
	 */
	default JsonAtKeysSpec values(K key) {
		Assert.notNull(key, "Key must not be null");
		return values(List.of(key));
	}

	/**
	 * Start building a JSON multi-value operation (i.e. multi-get) for the given {@code keys}.
	 *
	 * @param key must not be {@literal null}.
	 * @param additionalKeys must not be {@literal null}.
	 * @return a spec for specifying the multi-value operation.
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
	 * Start building a JSON multi-value operation (i.e. multi-get) for the given {@code keys}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return a spec for specifying the multi-value operation.
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
		 * @param values values to append, must not be empty or {@literal null}.
		 * @return a list where each element contains the new array length at matching paths.
		 * @see <a href="https://redis.io/commands/json.arrappend">Redis Documentation: JSON.ARRAPPEND</a>
		 */
		default List<@Nullable Long> append(Object... values) {
			return append(List.of(values));
		}

		/**
		 * Append the given {@code values} to the JSON array at the configured path.
		 *
		 * @param values values to append, must not be empty or {@literal null}.
		 * @return a list where each element contains the new array length at matching paths.
		 * @see <a href="https://redis.io/commands/json.arrappend">Redis Documentation: JSON.ARRAPPEND</a>
		 */
		List<@Nullable Long> append(Collection<? extends Object> values);

		/**
		 * Return the length of the JSON array at the configured path.
		 *
		 * @return a list where each element contains the array length at matching paths.
		 * @see <a href="https://redis.io/commands/json.arrlen">Redis Documentation: JSON.ARRLEN</a>
		 */
		List<@Nullable Long> length();

		/**
		 * Trim the JSON array so that it contains only the specified inclusive range of elements between {@code start} and
		 * {@code end} (i.e. remove everything outside the given range).
		 *
		 * @param start index of the first element to keep (previous elements are trimmed).
		 * @param end index of the last element to keep (following elements are trimmed), including the last element.
		 *          Negative values are interpreted as starting from the end.
		 * @return a list where each element contains the new array length at matching paths.
		 * @see <a href="https://redis.io/commands/json.arrtrim">Redis Documentation: JSON.ARRTRIM</a>
		 */
		List<@Nullable Long> trim(int start, int end);

		/**
		 * Return the first index of {@code value} within the JSON array at the configured path.
		 *
		 * @param value must not be {@literal null}.
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
		 * @return a list where each element contains the new array length at matching paths.
		 * @see <a href="https://redis.io/commands/json.arrinsert">Redis Documentation: JSON.ARRINSERT</a>
		 */
		default List<@Nullable Long> insert(Object... values) {
			return insert(List.of(values));
		}

		/**
		 * Insert {@code values} before the previously selected array index.
		 *
		 * @param values the values to insert, must not be empty or {@literal null}.
		 * @return a list where each element contains the new array length at matching paths.
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
		 * @return a list containing the updated boolean values for matching paths, or {@literal null} if the matching JSON
		 *         value is not a boolean.
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
		 * @return a list containing the string length for each matching path, or {@literal null} if the matching JSON value
		 *         is not a string.
		 * @see <a href="https://redis.io/commands/json.strlen">Redis Documentation: JSON.STRLEN</a>
		 */
		List<@Nullable Long> length();

		/**
		 * Append {@code value} to the JSON string values at the configured path.
		 *
		 * @param value the string value to append, must not be {@literal null}.
		 * @return a list containing the updated string length for each matching path, or {@literal null} if the matching
		 *         JSON value is not a string.
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
		 * @param value must not be {@literal null}.
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
		 * @return the JSON value wrapper.
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
		 * @param value must not be {@literal null}.
		 * @return {@literal true} if the value was written; {@literal false} otherwise.
		 * @see <a href="https://redis.io/commands/json.set">Redis Documentation: JSON.SET</a>
		 */
		Boolean set(T value);

		/**
		 * Set the JSON {@code value} at the configured path only if the path has one or more matches ({@code XX}).
		 *
		 * @param value must not be {@literal null}.
		 * @return {@literal true} if the value was written; {@literal false} otherwise.
		 */
		default Boolean setIfPresent(T value) {
			return conditional(JsonSetSpec::ifPresent).set(value);
		}

		/**
		 * Set the JSON {@code value} at the configured path only if the path has no matches ({@code NX}).
		 *
		 * @param value must not be {@literal null}.
		 * @return {@literal true} if the value was written; {@literal false} otherwise.
		 */
		default Boolean setIfAbsent(T value) {
			return conditional(JsonSetSpec::ifAbsent).set(value);
		}

	}

	/**
	 * Terminal step for multi-path or multi-key JSON read operations.
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
		 * @param jsonPath must not be {@literal null}.
		 * @return a new spec instance.
		 */
		@CheckReturnValue
		P path(String jsonPath);

	}

	/**
	 * A single JSON result value providing accessors to obtain the {@link #asString() raw} or {@link #as(Class)
	 * deserialized} result of a JSON command.
	 */
	interface JsonResult extends JsonValue {

		/**
		 * Decode this JSON value into the given target {@code type}.
		 * <p>
		 * RedisJSON returns a JSON array of matches for JSONPath queries, even when a query matches a single value. If
		 * {@code type} is not a {@link Iterable} or array type, the single matched value is transparently unwrapped from
		 * that array. A match count other than one raises a {@link SerializationException}.
		 *
		 * @param type must not be {@literal null}.
		 * @return the decoded value. Can be {@literal null} if the value is {@literal null} or the key is absent.
		 * @param <V> target type.
		 */
		<V> @Nullable V as(Class<V> type);

		/**
		 * Decode this JSON value into the given target {@code type}.
		 * <p>
		 * RedisJSON returns a JSON array of matches for JSONPath queries, even when a query matches a single value. If
		 * {@code type} is not a {@link Iterable} or array type, the single matched value is transparently unwrapped from
		 * that array. A match count other than one raises a {@link SerializationException}.
		 *
		 * @param type must not be {@literal null}.
		 * @return the decoded value. Can be {@literal null} if the value is {@literal null} or the key is absent.
		 * @param <V> target type.
		 */
		<V> @Nullable V as(ParameterizedTypeReference<V> type);

		/**
		 * Return the value as {@link String}.
		 *
		 * @return the value as {@link String} or {@literal null} if the value is {@literal null} or the key is absent.
		 */
		@Override
		default @Nullable String asString() {
			return as(String.class);
		}

		/**
		 * Map the raw JSON bytes of this value through the given {@code mapper}.
		 * <p>
		 * The mapping function is invoked with the raw JSON bytes if the result is not {@literal null}, however the JSON
		 * bytes may contain {@code null} if the projected value has a JSON {@literal null} value. The mapping function is
		 * not invoked if the result is absent (i.e. the key does not exist or the path did not match).
		 *
		 * @param mapper must not be {@literal null}.
		 * @return the mapped value.
		 * @param <U> mapped result type.
		 */
		<U extends @Nullable Object> U map(Function<? super byte[], ? extends U> mapper);

		/**
		 * Return whether this value is absent or represents JSON {@literal null}. An absent value indicates that the path
		 * did not match or the key did not exist.
		 *
		 * @return {@literal true} if this value is absent or represents {@literal null}; {@literal false} otherwise.
		 */
		boolean isNull();

	}

	/**
	 * A sequence of JSON result values providing accessors to obtain the {@link #asString() raw} or {@link #as(Class)
	 * deserialized} results of a JSON command. Elements are positionally correlated to the matching paths or input keys
	 * of the originating command.
	 */
	interface JsonResults extends Streamable<JsonResult> {

		/**
		 * Decode all JSON values into the given target {@code type}.
		 *
		 * @param type must not be {@literal null}.
		 * @return the decoded values.
		 * @param <V> target type.
		 */
		<V> List<@Nullable V> as(Class<V> type);

		/**
		 * Decode all JSON values into the given target {@code type}.
		 *
		 * @param type must not be {@literal null}.
		 * @return the decoded value.
		 * @param <V> target type.
		 */
		<V> List<@Nullable V> as(ParameterizedTypeReference<V> type);

		/**
		 * Return the JSON representation of all values as UTF-8 encoded {@link String Strings}.
		 *
		 * @return the JSON string representations.
		 */
		List<@Nullable String> asString();

		/**
		 * Return the JSON representation of all values as raw bytes.
		 *
		 * @return the raw JSON bytes.
		 */
		List<byte @Nullable []> asBytes();

		/**
		 * Return whether this result is absent or represents JSON {@literal null}. An absent result indicates that the key
		 * did not exist or the command yielded no values.
		 *
		 * @return {@literal true} if this result is absent or represents {@literal null}; {@literal false} otherwise.
		 */
		boolean isNull();

	}

	/**
	 * Steps for configuring a {@code JSON.SET} operation.
	 *
	 * @see <a href="https://redis.io/commands/json.set">Redis Documentation: JSON.SET</a>
	 */
	interface JsonSetSpec {

		/**
		 * Set the value unconditionally.
		 *
		 * @return this builder.
		 */
		JsonSetSpec always();

		/**
		 * Set the value only if the target path does not already exist ({@code NX}).
		 *
		 * @return this builder.
		 */
		JsonSetSpec ifAbsent();

		/**
		 * Set the value only if the target path already exists ({@code XX}).
		 *
		 * @return this builder.
		 */
		JsonSetSpec ifPresent();

	}

}
