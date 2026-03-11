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

import java.util.Collection;
import java.util.List;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.data.redis.core.json.JsonPath;
import org.springframework.util.Assert;

/**
 * Redis JSON specific operations, working on JSON documents stored in Redis.
 * <p>
 * This interface provides high-level operations for manipulating JSON data structures
 * using the Redis JSON module. Operations include reading and writing JSON values,
 * array manipulation, string operations, and numeric operations at specific JSON paths.
 * <p>
 * JSON paths follow the JSONPath syntax, where {@code $} represents the root element.
 * Use {@link JsonPath} to construct paths programmatically.
 *
 * @author Yordan Tsintsov
 * @see JsonPath
 * @see <a href="https://redis.io/docs/latest/develop/data-types/json/">Redis JSON Documentation</a>
 * @since 4.3
 */
@NullUnmarked
public interface JsonOperations<K> {

	/**
	 * Append {@code values} to the JSON array at {@code path} in the document stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @param values can be {@literal null}.
	 * @return a list where each element contains the new array length at matching paths,
	 *         or {@literal null} if the path does not exist or is not an array.
	 *         {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/json.arrappend/">Redis Documentation: JSON.ARRAPPEND</a>
	 */
	List<@Nullable Long> arrayAppend(@NonNull K key, @NonNull JsonPath path, Object... values);

	/**
	 * Search for the first occurrence of {@code value} in the JSON array at {@code path}.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @param value can be {@literal null}.
	 * @return a list where each element contains the index of the first occurrence of the value,
	 *         {@literal -1} if not found, or {@literal null} if the path does not exist or is not an array.
	 *         {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/json.arrindex/">Redis Documentation: JSON.ARRINDEX</a>
	 */
	List<@Nullable Long> arrayIndex(@NonNull K key, @NonNull JsonPath path, Object value);

	/**
	 * Search for the first occurrence of {@code value} in the JSON array at {@code path},
	 * starting from index {@code start}.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @param value can be {@literal null}.
	 * @param start the index to start searching from (inclusive).
	 * @return a list where each element contains the index of the first occurrence of the value,
	 *         {@literal -1} if not found, or {@literal null} if the path does not exist or is not an array.
	 *         {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/json.arrindex/">Redis Documentation: JSON.ARRINDEX</a>
	 */
	List<@Nullable Long> arrayIndex(@NonNull K key, @NonNull JsonPath path, Object value, long start);

	/**
	 * Search for the first occurrence of {@code value} in the JSON array at {@code path},
	 * within the range [{@code start}, {@code stop}).
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @param value can be {@literal null}.
	 * @param start the index to start searching from (inclusive).
	 * @param stop the index to stop searching at (exclusive). Use {@literal 0} to search to the end.
	 * @return a list where each element contains the index of the first occurrence of the value,
	 *         {@literal -1} if not found, or {@literal null} if the path does not exist or is not an array.
	 *         {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/json.arrindex/">Redis Documentation: JSON.ARRINDEX</a>
	 */
	List<@Nullable Long> arrayIndex(@NonNull K key, @NonNull JsonPath path, Object value, long start, long stop);

	/**
	 * Insert {@code values} into the JSON array at {@code path} before the element at {@code index}.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @param index the position to insert before. Negative values count from the end of the array.
	 * @param values can be {@literal null}.
	 * @return a list where each element contains the new array length at matching paths,
	 *         or {@literal null} if the path does not exist or is not an array.
	 *         {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/json.arrinsert/">Redis Documentation: JSON.ARRINSERT</a>
	 */
	List<@Nullable Long> arrayInsert(@NonNull K key, @NonNull JsonPath path, int index, Object... values);

	/**
	 * Get the length of the JSON array at {@code path}.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @return a list where each element contains the array length at matching paths,
	 *         or {@literal null} if the path does not exist or is not an array.
	 *         {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/json.arrlen/">Redis Documentation: JSON.ARRLEN</a>
	 */
	List<@Nullable Long> arrayLength(@NonNull K key, @NonNull JsonPath path);

	/**
	 * Remove and return the last element from the JSON array at {@code path}.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @param clazz must not be {@literal null}.
	 * @return a list where each element contains the popped value at matching paths,
	 *         or {@literal null} if the path does not exist, is not an array, or the array is empty.
	 *         {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/json.arrpop/">Redis Documentation: JSON.ARRPOP</a>
	 */
	<T> List<@Nullable T> arrayPop(@NonNull K key, @NonNull JsonPath path, @NonNull Class<T> clazz);

	/**
	 * Remove and return the element at {@code index} from the JSON array at {@code path}.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @param clazz must not be {@literal null}.
	 * @param index the position to pop from. Negative values count from the end of the array.
	 * @return a list where each element contains the popped value at matching paths,
	 *         or {@literal null} if the path does not exist, is not an array, or index is out of bounds.
	 *         {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/json.arrpop/">Redis Documentation: JSON.ARRPOP</a>
	 */
	<T> List<@Nullable T> arrayPop(@NonNull K key, @NonNull JsonPath path, @NonNull Class<T> clazz, int index);

	/**
	 * Remove and return the last element from the JSON array at {@code path}. Use this variant when the target value is a nested object.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @param typeRef must not be {@literal null}.
	 * @return a list where each element contains the popped value at matching paths,
	 *         or {@literal null} if the path does not exist, is not an array, or index is out of bounds.
	 *         {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/json.arrpop/">Redis Documentation: JSON.ARRPOP</a>
	 */
	<T> List<@Nullable T> arrayPop(@NonNull K key, @NonNull JsonPath path, @NonNull ParameterizedTypeReference<@NonNull T> typeRef);

	/**
	 * Remove and return the element at {@code index} from the JSON array at {@code path}. Use this variant when the target value is a nested object.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @param typeRef must not be {@literal null}.
	 * @param index the position to pop from. Negative values count from the end of the array.
	 * @return a list where each element contains the popped value at matching paths,
	 *         or {@literal null} if the path does not exist, is not an array, or index is out of bounds.
	 *         {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/json.arrpop/">Redis Documentation: JSON.ARRPOP</a>
	 */
	<T> List<@Nullable T> arrayPop(@NonNull K key, @NonNull JsonPath path, @NonNull ParameterizedTypeReference<@NonNull T> typeRef, int index);

	/**
	 * Trim the JSON array at {@code path} to contain only elements within the range [{@code start}, {@code stop}].
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @param start the start index (inclusive). Negative values count from the end of the array.
	 * @param stop the stop index (inclusive). Negative values count from the end of the array.
	 * @return a list where each element contains the new array length at matching paths,
	 *         or {@literal null} if the path does not exist or is not an array.
	 *         {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/json.arrtrim/">Redis Documentation: JSON.ARRTRIM</a>
	 */
	List<@Nullable Long> arrayTrim(@NonNull K key, @NonNull JsonPath path, int start, int stop);

	/**
	 * Clear container values (arrays/objects) and set numeric values to {@literal 0} at the root path
	 * of the document stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return the number of values cleared. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/json.clear/">Redis Documentation: JSON.CLEAR</a>
	 */
	Long clear(@NonNull K key);

	/**
	 * Clear container values (arrays/objects) and set numeric values to {@literal 0} at {@code path}
	 * in the document stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @return the number of values cleared. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/json.clear/">Redis Documentation: JSON.CLEAR</a>
	 */
	Long clear(@NonNull K key, @NonNull JsonPath path);

	/**
	 * Delete the entire JSON document stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return the number of paths deleted (0 or 1). {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/json.del/">Redis Documentation: JSON.DEL</a>
	 */
	Long delete(@NonNull K key);

	/**
	 * Delete the JSON value at {@code path} in the document stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @return the number of paths deleted. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/json.del/">Redis Documentation: JSON.DEL</a>
	 */
	Long delete(@NonNull K key, @NonNull JsonPath path);

	/**
	 * Get the entire JSON document stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param clazz must not be {@literal null}.
	 * @return the deserialized JSON document, or {@literal null} if the key does not exist.
	 *         {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/json.get/">Redis Documentation: JSON.GET</a>
	 */
	@Nullable <T> T get(@NonNull K key, @NonNull Class<T> clazz);

	/**
	 * Get the JSON value at {@code path} in the document stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param clazz must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @return a list where each element contains the value at matching paths,
	 *         or {@literal null} if the path does not exist.
	 *         {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/json.get/">Redis Documentation: JSON.GET</a>
	 */
	<T> List<@Nullable T> get(@NonNull K key, @NonNull Class<T> clazz, @NonNull JsonPath path);

	/**
	 * Get the JSON values at multiple {@code paths} in the document stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param clazz must not be {@literal null}.
	 * @param paths must not be {@literal null}.
	 * @return a list where each element contains the value at matching paths,
	 *         or {@literal null} if the path does not exist.
	 *         {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/json.get/">Redis Documentation: JSON.GET</a>
	 */
	<T> List<@Nullable T> get(@NonNull K key, @NonNull Class<T> clazz, @NonNull JsonPath @NonNull... paths);

	/**
	 * Get the entire JSON document stored at {@code key}. Use this variant when the target value is a nested object.
	 *
	 * @param key must not be {@literal null}.
	 * @param typeRef must not be {@literal null}.
	 * @return the deserialized JSON document, or {@literal null} if the key does not exist.
	 *         {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/json.get/">Redis Documentation: JSON.GET</a>
	 */
	@Nullable <T> T get(@NonNull K key, @NonNull ParameterizedTypeReference<@NonNull T> typeRef);

	/**
	 * Get the JSON value at {@code path} in the document stored at {@code key}. Use this variant when the target value is a nested object.
	 *
	 * @param key must not be {@literal null}.
	 * @param typeRef must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @return a list where each element contains the value at matching paths,
	 *         or {@literal null} if the path does not exist.
	 *         {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/json.get/">Redis Documentation: JSON.GET</a>
	 */
	<T> List<@Nullable T> get(@NonNull K key, @NonNull ParameterizedTypeReference<@NonNull T> typeRef, @NonNull JsonPath path);

	/**
	 * Get the JSON values at multiple {@code paths} in the document stored at {@code key}. Use this variant when the target value is a nested object.
	 *
	 * @param key must not be {@literal null}.
	 * @param typeRef must not be {@literal null}.
	 * @param paths must not be {@literal null}.
	 * @return a list where each element contains the value at matching paths,
	 *         or {@literal null} if the path does not exist.
	 *         {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/json.get/">Redis Documentation: JSON.GET</a>
	 */
	<T> List<@Nullable T> get(@NonNull K key, @NonNull ParameterizedTypeReference<@NonNull T> typeRef, @NonNull JsonPath @NonNull... paths);

	/**
	 * Increment the numeric value at {@code path} by {@code number}.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @param number must not be {@literal null}.
	 * @return a list where each element contains the new value at matching paths,
	 *         or {@literal null} if the path does not exist or is not a number.
	 *         {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/json.numincrby/">Redis Documentation: JSON.NUMINCRBY</a>
	 */
	List<@Nullable Number> increment(@NonNull K key, @NonNull JsonPath path, @NonNull Number number);

	/**
	 * Merge {@code value} into the root of the JSON document stored at {@code key}.
	 * <p>
	 * Merging follows RFC 7396 semantics: existing object keys are updated or added,
	 * and values set to {@literal null} are deleted.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return {@literal true} if the merge was successful, {@literal false} otherwise.
	 *         {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/json.merge/">Redis Documentation: JSON.MERGE</a>
	 */
	Boolean merge(@NonNull K key, Object value);

	/**
	 * Merge {@code value} into the JSON document at {@code path} stored at {@code key}.
	 * <p>
	 * Merging follows RFC 7396 semantics: existing object keys are updated or added,
	 * and values set to {@literal null} are deleted.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @param value can be {@literal null}.
	 * @return {@literal true} if the merge was successful, {@literal false} otherwise.
	 *         {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/json.merge/">Redis Documentation: JSON.MERGE</a>
	 */
	Boolean merge(@NonNull K key, @NonNull JsonPath path, Object value);

	/**
	 * Get the JSON documents stored at multiple {@code keys}.
	 *
	 * @param keys must not be {@literal null}.
	 * @param clazz must not be {@literal null}.
	 * @return a list of values at the root path for each key, with {@literal null} for keys that do not exist.
	 *         {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/json.mget/">Redis Documentation: JSON.MGET</a>
	 */
	<T> List<@Nullable T> multiGet(@NonNull Collection<K> keys, @NonNull Class<T> clazz);

	/**
	 * Get the JSON values at {@code path} from documents stored at multiple {@code keys}.
	 *
	 * @param keys must not be {@literal null}.
	 * @param clazz must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @return a list of values at the path for each key, with {@literal null} for keys where the path does not exist.
	 *         {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/json.mget/">Redis Documentation: JSON.MGET</a>
	 */
	<T> List<@Nullable T> multiGet(@NonNull Collection<K> keys, @NonNull Class<T> clazz, @NonNull JsonPath path);

	/**
	 * Get the JSON documents stored at multiple {@code keys}. Use this variant when the target value is a nested object.
	 *
	 * @param keys must not be {@literal null}.
	 * @param typeRef must not be {@literal null}.
	 * @return a list of values at the root path for each key, with {@literal null} for keys that do not exist.
	 *         {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/json.mget/">Redis Documentation: JSON.MGET</a>
	 */
	<T> List<@Nullable T> multiGet(@NonNull Collection<K> keys, @NonNull ParameterizedTypeReference<@NonNull T> typeRef);

	/**
	 * Get the JSON values at {@code path} from documents stored at multiple {@code keys}. Use this variant when the target value is a nested object.
	 *
	 * @param keys must not be {@literal null}.
	 * @param typeRef must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @return a list of values at the path for each key, with {@literal null} for keys where the path does not exist.
	 *         {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/json.mget/">Redis Documentation: JSON.MGET</a>
	 */
	<T> List<@Nullable T> multiGet(@NonNull Collection<K> keys, @NonNull ParameterizedTypeReference<@NonNull T> typeRef, @NonNull JsonPath path);

	/**
	 * Set JSON values at the specified keys and paths atomically.
	 *
	 * @param args must not be {@literal null}.
	 * @return {@literal true} if all values were set successfully, {@literal false} otherwise.
	 *         {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/json.mset/">Redis Documentation: JSON.MSET</a>
	 */
	Boolean multiSet(@NonNull List<JsonMultiSetArgs<K>> args);

	/**
	 * Set the JSON {@code value} at the root path of the document stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value can be {@literal null}.
	 * @return {@literal true} if the value was set successfully, {@literal false} otherwise.
	 *         {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/json.set/">Redis Documentation: JSON.SET</a>
	 */
	Boolean set(@NonNull K key, Object value);

	/**
	 * Set the JSON {@code value} at {@code path} in the document stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @param value can be {@literal null}.
	 * @return {@literal true} if the value was set successfully, {@literal false} otherwise.
	 *         {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/json.set/">Redis Documentation: JSON.SET</a>
	 */
	Boolean set(@NonNull K key, @NonNull JsonPath path, Object value);

	/**
	 * Set the JSON {@code value} at the root path only if the key does not already exist.
	 *
	 * @param key must not be {@literal null}.
	 * @param value can be {@literal null}.
	 * @return {@literal true} if the value was set, {@literal false} if the key already exists.
	 *         {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/json.set/">Redis Documentation: JSON.SET</a>
	 */
	Boolean setIfAbsent(@NonNull K key, Object value);

	/**
	 * Set the JSON {@code value} at the root path only if the key already exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param value can be {@literal null}.
	 * @return {@literal true} if the value was set, {@literal false} if the key does not exist.
	 *         {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/json.set/">Redis Documentation: JSON.SET</a>
	 */
	Boolean setIfPresent(@NonNull K key, Object value);

	/**
	 * Set the JSON {@code value} at {@code path} only if the path does not already exist.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @param value can be {@literal null}.
	 * @return {@literal true} if the value was set, {@literal false} if the path already exists.
	 *         {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/json.set/">Redis Documentation: JSON.SET</a>
	 */
	Boolean setIfPathAbsent(@NonNull K key, @NonNull JsonPath path, Object value);

	/**
	 * Set the JSON {@code value} at {@code path} only if the path already exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @param value can be {@literal null}.
	 * @return {@literal true} if the value was set, {@literal false} if the path does not exist.
	 *         {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/json.set/">Redis Documentation: JSON.SET</a>
	 */
	Boolean setIfPathPresent(@NonNull K key, @NonNull JsonPath path, Object value);

	/**
	 * Append {@code value} to the JSON string at {@code path}.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return a list where each element contains the new string length at matching paths,
	 *         or {@literal null} if the path does not exist or is not a string.
	 *         {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/json.strappend/">Redis Documentation: JSON.STRAPPEND</a>
	 */
	List<@Nullable Long> stringAppend(@NonNull K key, @NonNull JsonPath path, @NonNull String value);

	/**
	 * Get the length of the JSON string at {@code path}.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @return a list where each element contains the string length at matching paths,
	 *         or {@literal null} if the path does not exist or is not a string.
	 *         {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/json.strlen/">Redis Documentation: JSON.STRLEN</a>
	 */
	List<@Nullable Long> stringLength(@NonNull K key, @NonNull JsonPath path);

	/**
	 * Toggle the JSON boolean value at {@code path}.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @return a list where each element contains the new boolean value at matching paths,
	 *         or {@literal null} if the path does not exist or is not a boolean.
	 *         {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/json.toggle/">Redis Documentation: JSON.TOGGLE</a>
	 */
	List<@Nullable Boolean> toggle(@NonNull K key, @NonNull JsonPath path);

	/**
	 * Get the JSON type at the root path of the document stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return a list containing the type at the root path. Returns an empty list if the key does not exist.
	 *         {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/json.type/">Redis Documentation: JSON.TYPE</a>
	 */
	List<Class<?>> type(@NonNull K key);

	/**
	 * Get the JSON type at {@code path} in the document stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @return a list where each element contains the type at matching paths.
	 *         Returns an empty list if the path does not exist.
	 *         {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/json.type/">Redis Documentation: JSON.TYPE</a>
	 */
	List<Class<?>> type(@NonNull K key, @NonNull JsonPath path);

	/**
	 * @return the underlying {@link RedisOperations} used to execute commands.
	 */
	@NonNull
	RedisOperations<K, ?> getOperations();

	/**
	 * Arguments for {@link #multiSet(List)} operation.
	 *
	 * @param key the key, must not be {@literal null}.
	 * @param path the JSON path, must not be {@literal null}.
	 * @param value can be {@literal null}.
	 */
	record JsonMultiSetArgs<K>(@NonNull K key, @NonNull JsonPath path, Object value) {

		/**
		 * Creates a new {@link JsonMultiSetArgs} with validation.
		 *
		 * @param key must not be {@literal null}.
		 * @param path must not be {@literal null}.
		 * @param value can be {@literal null}.
		 */
		public JsonMultiSetArgs {
			Assert.notNull(key, "Key must not be null");
			Assert.notNull(path, "Path must not be null");
		}

		/**
		 * Creates a new {@link JsonMultiSetArgs} for the root path.
		 *
		 * @param key must not be {@literal null}.
		 * @param value must not be {@literal null}.
		 */
		public JsonMultiSetArgs(K key, Object value) {
			this(key, JsonPath.root(), value);
		}

	}

}
