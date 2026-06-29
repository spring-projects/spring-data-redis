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
package org.springframework.data.redis.connection;

import java.util.List;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.springframework.data.redis.connection.json.JsonPath;
import org.springframework.data.redis.connection.json.JsonValue;

/**
 * JSON commands supported by Redis.
 * <p>
 * Many methods in this interface return a {@link List} rather than a single value. This is because
 * JSONPath expressions can match multiple values within a document, and each match produces its own
 * result. For example, given the following document stored at key {@code user:1}:
 * <pre>{@code
 * {
 *   "groups": [
 *     { "name": "admins",  "roles": ["read", "write", "delete"] },
 *     { "name": "editors", "roles": ["read", "write"] },
 *     { "name": "viewers", "roles": ["read"] }
 *   ]
 * }
 * }</pre>
 * Calling {@code JSON.ARRINDEX user:1 $.groups[*].roles "write"} returns:
 * <pre>{@code
 * [1, 1, -1]
 * }</pre>
 * The path {@code $.groups[*].roles} matches three arrays, so the result contains one entry per
 * match: index {@code 1} for the first two arrays (where {@code "write"} appears at position 1)
 * and {@code -1} for the third array (where {@code "write"} is not found).
 *
 * @author Yordan Tsintsov
 * @see RedisCommands
 * @since 4.2
 */
@NullUnmarked
public interface RedisJsonCommands {

	/**
	 * Append the JSON rawJsonValues into the array at path after the last element in it.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @param values must not be {@literal null}.
	 * @return a list where each element contains the new length of the array or {@literal null} if path does not exist.
	 * @see <a href="https://redis.io/docs/latest/commands/json.arrappend/">Redis Documentation: JSON.ARRAPPEND</a>
	 * @since 4.2
	 */
	List<Long> jsonArrAppend(byte @NonNull [] key, @NonNull JsonPath path, @NonNull JsonValue @NonNull... values);

	/**
	 * Search for the first occurrence of a JSON rawJsonValue in an array.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return a list where each element contains the index of the first occurrence of the rawJsonValue, {@code -1} if not found,
	 * 		or {@literal null} if the matched rawJsonValue is not an array. Returns an empty list if the path does not match any rawJsonValue.
	 * @see <a href="https://redis.io/docs/latest/commands/json.arrindex/">Redis Documentation: JSON.ARRINDEX</a>
	 * @since 4.2
	 */
	List<Long> jsonArrIndex(byte @NonNull [] key, @NonNull JsonPath path, @NonNull JsonValue value);

	/**
	 * Insert the {@code rawJsonValues} into the array at {@code path} before {@code index}.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @param index to insert before.
	 * @param values must not be {@literal null}.
	 * @return a list where each element contains the new length of the array after the insertion or {@literal null} if path does not exist.
	 * @see <a href="https://redis.io/docs/latest/commands/json.arrinsert/">Redis Documentation: JSON.ARRINSERT</a>
	 * @since 4.2
	 */
	List<Long> jsonArrInsert(byte @NonNull [] key, @NonNull JsonPath path, int index, @NonNull JsonValue @NonNull... values);

	/**
	 * Get the length of the array at the given path.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @return a list where each element contains the length of the array or {@literal null} if path does not exist.
	 * @see <a href="https://redis.io/docs/latest/commands/json.arrlen/">Redis Documentation: JSON.ARRLEN</a>
	 * @since 4.2
	 */
	List<Long> jsonArrLen(byte @NonNull [] key, @NonNull JsonPath path);

	/**
	 * Trim an array so that it contains only the specified inclusive range of elements.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @param start index to start trimming from ({@code inclusive}).
	 * @param stop index to stop trimming at ({@code inclusive}).
	 * @return a list where each element contains the length of the array after the trim or {@literal null} if path does not exist.
	 * @see <a href="https://redis.io/docs/latest/commands/json.arrtrim/">Redis Documentation: JSON.ARRTRIM</a>
	 * @since 4.2
	 */
	List<Long> jsonArrTrim(byte @NonNull [] key, @NonNull JsonPath path, int start, int stop);

	/**
	 * Clear container values (arrays/objects) and set numeric values to 0 at the root path of the given key.
	 *
	 * @param key must not be {@literal null}.
	 * @return the number of paths cleared.
	 * @see <a href="https://redis.io/docs/latest/commands/json.clear/">Redis Documentation: JSON.CLEAR</a>
	 * @since 4.2
	 */
	default Long jsonClear(byte @NonNull [] key) {
		return jsonClear(key, JsonPath.root());
	}

	/**
	 * Clear container values (arrays/objects) and set numeric values to 0 at the given key and path.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @return the number of paths cleared.
	 * @see <a href="https://redis.io/docs/latest/commands/json.clear/">Redis Documentation: JSON.CLEAR</a>
	 * @since 4.2
	 */
	Long jsonClear(byte @NonNull [] key, @NonNull JsonPath path);

	/**
	 * Delete the JSON value at the root path of the given key.
	 *
	 * @param key must not be {@literal null}.
	 * @return the number of paths deleted.
	 * @see <a href="https://redis.io/docs/latest/commands/json.del/">Redis Documentation: JSON.DEL</a>
	 * @since 4.2
	 */
	default Long jsonDel(byte @NonNull [] key) {
		return jsonDel(key, JsonPath.root());
	}

	/**
	 * Delete the JSON value at the given key and path.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @return the number of paths deleted.
	 * @see <a href="https://redis.io/docs/latest/commands/json.del/">Redis Documentation: JSON.DEL</a>
	 * @since 4.2
	 */
	Long jsonDel(byte @NonNull [] key, @NonNull JsonPath path);

	/**
	 * Get the JSON value at the root path of the given key.
	 *
	 * @param key must not be {@literal null}.
	 * @return a JSON-serialized string. When a single path is given, returns the value at that path.
	 * 			When multiple paths are given, returns a JSON object with each path as a key. Returns {@code null} if the key does not exist.
	 * @see <a href="https://redis.io/docs/latest/commands/json.get/">Redis Documentation: JSON.GET</a>
	 * @since 4.2
	 */
	default String jsonGet(byte @NonNull [] key) {
		return jsonGet(key, JsonPath.root());
	}

	/**
	 * Get the JSON values at the given key and paths.
	 *
	 * @param key must not be {@literal null}.
	 * @param paths must not be {@literal null}.
	 * @return a JSON-serialized string. When a single path is given, returns the value at that path.
	 * 			When multiple paths are given, returns a JSON object with each path as a key. Returns {@code null} if the key does not exist.
	 * @see <a href="https://redis.io/docs/latest/commands/json.get/">Redis Documentation: JSON.GET</a>
	 * @since 4.2
	 */
	String jsonGet(byte @NonNull [] key, @NonNull JsonPath @NonNull... paths);

	/**
	 * Merge the JSON rawJsonValue at the root path of the given {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return {@literal true} if the key was merged, {@literal false} otherwise.
	 * @see <a href="https://redis.io/docs/latest/commands/json.merge/">Redis Documentation: JSON.MERGE</a>
	 * @since 4.2
	 */
	default Boolean jsonMerge(byte @NonNull [] key, @NonNull JsonValue value) {
		return jsonMerge(key, JsonPath.root(), value);
	}

	/**
	 * Merge the JSON rawJsonValue at the given key and path.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return {@literal true} if the key was merged, {@literal false} otherwise.
	 * @see <a href="https://redis.io/docs/latest/commands/json.merge/">Redis Documentation: JSON.MERGE</a>
	 * @since 4.2
	 */
	Boolean jsonMerge(byte @NonNull [] key, @NonNull JsonPath path, @NonNull JsonValue value);

	/**
	 * Get the JSON values at the root path of the given keys.
	 *
	 * @param keys must not be {@literal null}.
	 * @return list of root JSON values or {@literal null} if path does not exist.
	 * @see <a href="https://redis.io/docs/latest/commands/json.mget/">Redis Documentation: JSON.MGET</a>
	 * @since 4.2
	 */
	default List<String> jsonMGet(byte @NonNull [] @NonNull... keys) {
		return jsonMGet(JsonPath.root(), keys);
	}

	/**
	 * Get the JSON values at the given path for the given keys.
	 *
	 * @param path must not be {@literal null}.
	 * @param keys must not be {@literal null}.
	 * @return list of JSON values or {@literal null} if path does not exist.
	 * @see <a href="https://redis.io/docs/latest/commands/json.mget/">Redis Documentation: JSON.MGET</a>
	 * @since 4.2
	 */
	List<String> jsonMGet(@NonNull JsonPath path, byte @NonNull [] @NonNull... keys);

	/**
	 * Set the JSON rawJsonValue at the root path of the given key.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return {@literal true} if the key was set, {@literal false} otherwise.
	 * @see <a href="https://redis.io/docs/latest/commands/json.set/">Redis Documentation: JSON.SET</a>
	 * @since 4.2
	 */
	default Boolean jsonSet(byte @NonNull [] key, @NonNull JsonValue value) {
		return jsonSet(key, JsonPath.root(), value, JsonSetCondition.upsert());
	}

	/**
	 * Set the JSON rawJsonValue at the given key and path.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @param condition must not be {@literal null}.
	 * @return {@literal true} if the key was set, {@literal false} otherwise.
	 * @see <a href="https://redis.io/docs/latest/commands/json.set/">Redis Documentation: JSON.SET</a>
	 * @since 4.2
	 */
	Boolean jsonSet(byte @NonNull [] key, @NonNull JsonPath path, @NonNull JsonValue value, @NonNull JsonSetCondition condition);

	/**
	 * Append a string value to the JSON string at the given path.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @param value must not be {@literal null}. Value must be JSON encoded.
	 * @return a list where each element is the new string length or {@literal null} if path does not exist.
	 * @see <a href="https://redis.io/docs/latest/commands/json.strappend/">Redis Documentation: JSON.STRAPPEND</a>
	 * @since 4.2
	 */
	List<Long> jsonStrAppend(byte @NonNull [] key, @NonNull JsonPath path, @NonNull String value);

	/**
	 * Get the length of the JSON string value at the given path.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @return a list where each element is the string length or {@literal null} if path does not exist.
	 * @see <a href="https://redis.io/docs/latest/commands/json.strlen/">Redis Documentation: JSON.STRLEN</a>
	 * @since 4.2
	 */
	List<Long> jsonStrLen(byte @NonNull [] key, @NonNull JsonPath path);

	/**
	 * Toggle boolean values at the given key and path.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @return a list where each element is the new boolean value after toggling, or {@literal null} if the path does not exist.
	 * @see <a href="https://redis.io/docs/latest/commands/json.toggle/">Redis Documentation: JSON.TOGGLE</a>
	 * @since 4.2
	 */
	List<Boolean> jsonToggle(byte @NonNull [] key, @NonNull JsonPath path);

	/**
	 * Get the JSON type at the root path of the given key.
	 *
	 * @param key must not be {@literal null}.
	 * @return a list where each element is the type at the given path.
	 * @see <a href="https://redis.io/docs/latest/commands/json.type/">Redis Documentation: JSON.TYPE</a>
	 * @since 4.2
	 */
	default List<JsonType> jsonType(byte @NonNull [] key) {
		return jsonType(key, JsonPath.root());
	}

	/**
	 * Get the JSON type at the given key and path.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @return a list where each element is the type at the given path.
	 * @see <a href="https://redis.io/docs/latest/commands/json.type/">Redis Documentation: JSON.TYPE</a>
	 * @since 4.2
	 */
	List<JsonType> jsonType(byte @NonNull [] key, @NonNull JsonPath path);

	/**
	 * Enum representation of the JSON types provided by Redis JSON API.
	 */
	enum JsonType {

		STRING,
		NUMBER,
		BOOLEAN,
		OBJECT,
		ARRAY

	}

}
