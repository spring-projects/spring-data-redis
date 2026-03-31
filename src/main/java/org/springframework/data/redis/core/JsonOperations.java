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

import io.lettuce.core.json.JsonType;

import java.nio.charset.Charset;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import org.jspecify.annotations.Nullable;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.data.domain.Range;
import org.springframework.data.util.Streamable;

/**
 * Redis operations for JSON values providing a fluent API for flexible JSON result consumption.
 * <p>
 * Typed entry points bound to a key allow configuring the command and running it by calling a terminal method returning
 * the command result, for example:
 *
 * <pre class="code">
 * operations.key("key").set("value");
 *
 * operations.key("key").path("$..name").setIfAbsent("Doe");
 *
 * operations.array("key").path("$.names").index(2).insert("John")
 * Person person = operations.key("key").get().as(Person.class);
 * </pre>
 * <p>
 * JSON path expressions follow the
 * <a href="https://redis.io/docs/latest/develop/data-types/json/path/#jsonpath-syntax">RedisJSON</a> path syntax.
 * Unless specified otherwise, results are positionally correlated to matching paths: each element in the returned
 * {@link List} corresponds to a matching path, and {@literal null} elements indicate paths that did not match or
 * pointed to an incompatible JSON type.
 *
 * @param <K> the Redis key type.
 * @author Mark Paluch
 * @since 4.1
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
	 * Start building a JSON number operation for the given {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return a spec for specifying the numeric operation.
	 */
	JsonNumberSpec number(K key);

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
	JsonAtKeySpec key(K key);

	/**
	 * Retrieve the JSON value for the given {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return the JSON value for the given key.
	 */
	default JsonValue get(K key) {
		return key(key).get();
	}

	/**
	 * Set the {@code key} to a JSON {@code value}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return {@literal true} if the value was written; {@literal false} otherwise.
	 */
	default Boolean set(K key, Object value) {
		return key(key).set(value);
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
	 * Create {@code JsonOperations} for the given {@link RedisOperations}.
	 *
	 * @param operations the {@link RedisOperations} to obtain connections and the key serializer from.
	 * @return the JSON operations view.
	 */
	static <K> JsonOperations<K> create(RedisOperations<K, ?> operations) {
		return null;
	}

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
		 * @param values values to append.
		 * @return a list where each element contains the new array length at matching paths, or {@literal null} if the path
		 *         does not exist or is not an array.
		 * @see <a href="https://redis.io/commands/json.arrappend">Redis Documentation: JSON.ARRAPPEND</a>
		 */
		List<@Nullable Long> append(Object... values);

		/**
		 * Return the length of the JSON array at the configured path.
		 *
		 * @return a list where each element contains the array length at matching paths, or {@literal null} if the path
		 *         does not exist or is not an array.
		 * @see <a href="https://redis.io/commands/json.arrlen">Redis Documentation: JSON.ARRLEN</a>
		 */
		List<@Nullable Long> length();

		/**
		 * Pop and return the last element from the JSON array at the configured path.
		 *
		 * @return the popped values where each element corresponds to a matching path, or a {@linkplain JsonValue#isNull()
		 *         null value} if the path does not exist, is not an array, or the array is empty.
		 * @see <a href="https://redis.io/commands/json.arrpop">Redis Documentation: JSON.ARRPOP</a>
		 */
		JsonValues pop();

		/**
		 * Trim the JSON array so that it contains only the specified inclusive range of elements between {@code start} and
		 * {@code end} (i.e. remove everything outside of the given range).
		 *
		 * @param start index of the first element to keep (previous elements are trimmed).
		 * @param end index of the last element to keep (following elements are trimmed), including the last element.
		 *          Negative values are interpreted as starting from the end.
		 * @return a list where each element contains the new array length at matching paths, or {@literal null} if the path
		 *         does not exist or is not an array.
		 * @see <a href="https://redis.io/commands/json.arrtrim">Redis Documentation: JSON.ARRTRIM</a>
		 */
		List<@Nullable Long> trim(long start, long end);

		/**
		 * Return the first index of {@code value} within the JSON array at the configured path.
		 *
		 * @param value must not be {@literal null}.
		 * @return a list containing the first zero-based index for each matching path, or {@code -1} if the value is not
		 *         contained in the corresponding array.
		 * @see <a href="https://redis.io/commands/json.arrindex">Redis Documentation: JSON.ARRINDEX</a>
		 */
		List<Long> indexOf(Object value);

		/**
		 * Restrict subsequent array search operations to the given index {@code range}.
		 *
		 * @param range must not be {@literal null}.
		 * @return a spec scoped to the given range for array search operations.
		 */
		JsonArrayWithin within(Range<Long> range);

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
		 * Insert {@code value} before the previously selected array index.
		 *
		 * @param value must not be {@literal null}.
		 * @return a list where each element contains the new array length at matching paths, or {@literal null} if the path
		 *         does not exist or is not an array.
		 * @see <a href="https://redis.io/commands/json.arrinsert">Redis Documentation: JSON.ARRINSERT</a>
		 */
		List<@Nullable Long> insert(Object value);

		/**
		 * Pop and return the element at the previously selected array index.
		 *
		 * @return the popped value, or a {@link JsonValue#isNull() null value} if the path does not exist, is not an array,
		 *         or the array is empty at the selected index.
		 * @see <a href="https://redis.io/commands/json.arrpop">Redis Documentation: JSON.ARRPOP</a>
		 */
		JsonValue pop();

	}

	/**
	 * Specification for JSON array search operations constrained to a particular index range.
	 *
	 * @see <a href="https://redis.io/commands/json.arrindex">Redis Documentation: JSON.ARRINDEX</a>
	 */
	interface JsonArrayWithin {

		/**
		 * Return the first index of {@code value} within the configured array range.
		 *
		 * @param value must not be {@literal null}.
		 * @return a list containing the first zero-based index for each matching path, or {@code -1} if the value is not
		 *         contained in the corresponding array range.
		 */
		List<Long> indexOf(Object value);

	}

	/**
	 * Specification for JSON boolean operations bound to a particular {@code key}.
	 *
	 * @see <a href="https://redis.io/commands/json.toggle">Redis Documentation: JSON.TOGGLE</a>
	 */
	interface JsonBooleanSpec extends JsonKeySupport<JsonBooleanSpec>, JsonSetSupport<Boolean, JsonBooleanSpec> {

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
	 * Specification for JSON number operations bound to a particular {@code key}.
	 *
	 * @see <a href="https://redis.io/commands/json.numincrby">Redis Documentation: JSON.NUMINCRBY</a>
	 */
	interface JsonNumberSpec extends JsonKeySupport<JsonNumberSpec>, JsonSetSupport<Number, JsonNumberSpec> {

		/**
		 * Increment the numeric values at the configured path by {@code delta}.
		 *
		 * @param delta must not be {@literal null}.
		 * @return a list containing the incremented numeric values for matching paths, or {@literal null} if the matching
		 *         JSON value is not numeric.
		 * @see <a href="https://redis.io/commands/json.numincrby">Redis Documentation: JSON.NUMINCRBY</a>
		 */
		List<@Nullable Number> increment(Number delta);

	}

	/**
	 * Specification for JSON string operations bound to a particular {@code key}.
	 */
	interface JsonStringSpec extends JsonKeySupport<JsonStringSpec>, JsonSetSupport<String, JsonStringSpec> {

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
		 * @param value must not be {@literal null}.
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
	interface JsonAtKeySpec extends JsonKeySupport<JsonAtKeySpec>, JsonSetSupport<Object, JsonAtKeySpec> {

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
		List<JsonType> getType();

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
		JsonValue get();

		/**
		 * Select additional JSON {@code paths} to retrieve from the same key.
		 *
		 * @param paths must not be {@literal null}.
		 * @return a terminal spec to execute the multi-path read.
		 * @see <a href="https://redis.io/commands/json.get">Redis Documentation: JSON.GET</a>
		 */
		JsonMultiGetSpec paths(String... paths);

		/**
		 * Select additional JSON {@code paths} to retrieve from the same key.
		 *
		 * @param paths must not be {@literal null}.
		 * @return a terminal spec to execute the multi-path read.
		 * @see <a href="https://redis.io/commands/json.get">Redis Documentation: JSON.GET</a>
		 */
		JsonMultiGetSpec paths(Collection<String> paths);

	}

	/**
	 * Common support for setting JSON values at the currently configured path.
	 *
	 * @param <T> value type.
	 * @param <S> self-type used for fluent method chaining.
	 * @see <a href="https://redis.io/commands/json.set">Redis Documentation: JSON.SET</a>
	 */
	interface JsonSetSupport<T, S extends JsonSetSupport<T, S>> {

		/**
		 * Apply a condition to the set operation through a {@link JsonSetSpec}.
		 *
		 * @param consumer callback to configure the condition, must not be {@literal null}.
		 * @return this spec for further configuration.
		 */
		S conditional(Consumer<JsonSetSpec> consumer);

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
		JsonValues get();
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
		 * @return this builder.
		 */
		P root();

		/**
		 * Select the JSON path to operate on.
		 *
		 * @param jsonPath must not be {@literal null}.
		 * @return this builder.
		 */
		P path(String jsonPath);

	}

	/**
	 * A single JSON result value providing accessors to obtain the {@link #asString() raw} or {@link #as(Class)
	 * deserialized} result of a JSON command.
	 */
	interface JsonValue {

		/**
		 * Decode this JSON value into the given target {@code type}.
		 *
		 * @param type must not be {@literal null}.
		 * @return the decoded value.
		 * @param <V> target type.
		 */
		<V> V as(Class<V> type);

		/**
		 * Decode this JSON value into the given target {@code type}.
		 *
		 * @param type must not be {@literal null}.
		 * @return the decoded value.
		 * @param <V> target type.
		 */
		<V> V as(ParameterizedTypeReference<V> type);

		/**
		 * Return the JSON representation of this value as a UTF-8 encoded {@link String}.
		 *
		 * @return the JSON representation.
		 */
		String asString();

		/**
		 * Return the JSON representation of this value as a {@link String} using the given {@code charset}.
		 *
		 * @param charset must not be {@literal null}.
		 * @return the JSON representation.
		 */
		String asString(Charset charset);

		/**
		 * Map the raw JSON bytes of this value through the given {@code mapper}.
		 *
		 * @param mapper must not be {@literal null}.
		 * @return the mapped value.
		 * @param <U> mapped result type.
		 */
		<U> U map(Function<? super byte[], ? extends U> mapper);

		/**
		 * Return the JSON representation of this value as raw bytes.
		 *
		 * @return the raw JSON bytes.
		 */
		byte[] asBytes();

		/**
		 * Return whether this value is absent or represents JSON {@code null}. An absent value indicates that the path did
		 * not match or the key did not exist.
		 *
		 * @return {@literal true} if this value is absent or represents {@code null}; {@literal false} otherwise.
		 */
		boolean isNull();

		/**
		 * Return the JSON representation of this value as a string, primarily for debugging purposes. Prefer
		 * {@link #asString()} or {@link #asBytes()} for programmatic access.
		 *
		 * @return the JSON representation, or {@literal null} if this value {@link #isNull() is null}.
		 */
		@Nullable
		String toString();

	}

	/**
	 * A sequence of JSON result values providing accessors to obtain the {@link #asString() raw} or {@link #as(Class)
	 * deserialized} results of a JSON command. Elements are positionally correlated to the matching paths or input keys
	 * of the originating command.
	 */
	interface JsonValues extends Streamable<JsonValue> {

		/**
		 * Decode all JSON values into the given target {@code type}.
		 *
		 * @param type must not be {@literal null}.
		 * @return the decoded values.
		 * @param <V> target type.
		 */
		<V> List<V> as(Class<V> type);

		/**
		 * Return the JSON representation of all values as UTF-8 encoded {@link String Strings}.
		 *
		 * @return the JSON string representations.
		 */
		default List<String> asString() {
			return as(String.class);
		}

		/**
		 * Return the JSON representation of all values as raw bytes.
		 *
		 * @return the raw JSON bytes.
		 */
		default List<byte[]> asBytes() {
			return as(byte[].class);
		}

		/**
		 * Return whether this result is absent or represents JSON {@code null}. An absent result indicates that the key did
		 * not exist or the command yielded no values.
		 *
		 * @return {@literal true} if this result is absent or represents {@code null}; {@literal false} otherwise.
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
