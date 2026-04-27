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

import org.jspecify.annotations.Nullable;

import java.util.List;

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
 * @since 4.1
 */
public interface RedisJsonCommands {

	String ROOT_PATH = "$";

	/**
	 * Append the JSON values into the array at path after the last element in it.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @param values must not be {@literal null}. {@literal null} values should be represented as JSON "null" values.
	 * @return a list where each element contains the new length of the array or {@literal null} if path does not exist.
	 * @see <a href="https://redis.io/docs/latest/commands/json.arrappend/">Redis Documentation: JSON.ARRAPPEND</a>
	 * @since 4.1
	 */
	List<@Nullable Long> jsonArrAppend(byte[] key, String path, String... values);

	/**
	 * Search for the first occurrence of a JSON value in an array.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @param value must not be {@literal null}. {@literal null} values should be represented as JSON "null" values.
	 * @return a list where each element contains the index of the first occurrence of the value, {@code -1} if not found,
	 * 		or {@literal null} if the matched value is not an array. Returns an empty list if the path does not match any value.
	 * @see <a href="https://redis.io/docs/latest/commands/json.arrindex/">Redis Documentation: JSON.ARRINDEX</a>
	 * @since 4.1
	 */
	List<@Nullable Long> jsonArrIndex(byte[] key, String path, String value);

	/**
	 * Insert the {@code values} into the array at {@code path} before {@code index}.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @param index to insert before.
	 * @param values must not be {@literal null}. {@literal null} values should be represented as JSON "null" values.
	 * @return a list where each element contains the new length of the array after the insertion or {@literal null} if path does not exist.
	 * @see <a href="https://redis.io/docs/latest/commands/json.arrinsert/">Redis Documentation: JSON.ARRINSERT</a>
	 * @since 4.1
	 */
	List<@Nullable Long> jsonArrInsert(byte[] key, String path, int index, String... values);

	/**
	 * Get the length of the array at the given path.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @return a list where each element contains the length of the array or {@literal null} if path does not exist.
	 * @see <a href="https://redis.io/docs/latest/commands/json.arrlen/">Redis Documentation: JSON.ARRLEN</a>
	 * @since 4.1
	 */
	List<@Nullable Long> jsonArrLen(byte[] key, String path);

	/**
	 * Trim an array so that it contains only the specified inclusive range of elements.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @param start index to start trimming from ({@code inclusive}).
	 * @param stop index to stop trimming at ({@code inclusive}).
	 * @return a list where each element contains the length of the array after the trim or {@literal null} if path does not exist.
	 * @see <a href="https://redis.io/docs/latest/commands/json.arrtrim/">Redis Documentation: JSON.ARRTRIM</a>
	 * @since 4.1
	 */
	List<@Nullable Long> jsonArrTrim(byte[] key, String path, int start, int stop);

	/**
	 * Clear container values (arrays/objects) and set numeric values to 0 at the root path of the given key.
	 *
	 * @param key must not be {@literal null}.
	 * @return the number of paths cleared.
	 * @see <a href="https://redis.io/docs/latest/commands/json.clear/">Redis Documentation: JSON.CLEAR</a>
	 * @since 4.1
	 */
	default Long jsonClear(byte[] key) {
		return jsonClear(key, ROOT_PATH);
	}

	/**
	 * Clear container values (arrays/objects) and set numeric values to 0 at the given key and path.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @return the number of paths cleared.
	 * @see <a href="https://redis.io/docs/latest/commands/json.clear/">Redis Documentation: JSON.CLEAR</a>
	 * @since 4.1
	 */
	Long jsonClear(byte[] key, String path);

	/**
	 * Delete the JSON value at the root path of the given key.
	 *
	 * @param key must not be {@literal null}.
	 * @return the number of paths deleted.
	 * @see <a href="https://redis.io/docs/latest/commands/json.del/">Redis Documentation: JSON.DEL</a>
	 * @since 4.1
	 */
	default Long jsonDel(byte[] key) {
		return jsonDel(key, ROOT_PATH);
	}

	/**
	 * Delete the JSON value at the given key and path.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @return the number of paths deleted.
	 * @see <a href="https://redis.io/docs/latest/commands/json.del/">Redis Documentation: JSON.DEL</a>
	 * @since 4.1
	 */
	Long jsonDel(byte[] key, String path);

	/**
	 * Get the JSON value at the root path of the given key.
	 *
	 * @param key must not be {@literal null}.
	 * @return the JSON value at the root path, or {@literal null} if the key does not exist.
	 * @see <a href="https://redis.io/docs/latest/commands/json.get/">Redis Documentation: JSON.GET</a>
	 * @since 4.1
	 */
	default @Nullable String jsonGet(byte[] key) {
		return jsonGet(key, ROOT_PATH);
	}

	/**
	 * Get the JSON values at the given key and paths.
	 *
	 * @param key must not be {@literal null}.
	 * @param paths must not be {@literal null}.
	 * @return list where each element is a JSON value or {@literal null} if path does not exist.
	 * @see <a href="https://redis.io/docs/latest/commands/json.get/">Redis Documentation: JSON.GET</a>
	 * @since 4.1
	 */
	@Nullable String jsonGet(byte[] key, String... paths);

	/**
	 * Merge the JSON value at the root path of the given {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}. {@literal null} values should be represented as JSON "null" values.
	 * @return {@literal true} if the key was merged, {@literal false} otherwise.
	 * @see <a href="https://redis.io/docs/latest/commands/json.merge/">Redis Documentation: JSON.MERGE</a>
	 * @since 4.1
	 */
	default Boolean jsonMerge(byte[] key, String value) {
		return jsonMerge(key, ROOT_PATH, value);
	}

	/**
	 * Merge the JSON value at the given key and path.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @param value must not be {@literal null}. {@literal null} values should be represented as JSON "null" values.
	 * @return {@literal true} if the key was merged, {@literal false} otherwise.
	 * @see <a href="https://redis.io/docs/latest/commands/json.merge/">Redis Documentation: JSON.MERGE</a>
	 * @since 4.1
	 */
	Boolean jsonMerge(byte[] key, String path, String value);

	/**
	 * Get the JSON values at the root path of the given keys.
	 *
	 * @param keys must not be {@literal null}.
	 * @return list of root JSON values or {@literal null} if path does not exist.
	 * @see <a href="https://redis.io/docs/latest/commands/json.mget/">Redis Documentation: JSON.MGET</a>
	 * @since 4.1
	 */
	default List<@Nullable String> jsonMGet(byte[]... keys) {
		return jsonMGet(ROOT_PATH, keys);
	}

	/**
	 * Get the JSON values at the given path for the given keys.
	 *
	 * @param path must not be {@literal null}.
	 * @param keys must not be {@literal null}.
	 * @return list of JSON values or {@literal null} if path does not exist.
	 * @see <a href="https://redis.io/docs/latest/commands/json.mget/">Redis Documentation: JSON.MGET</a>
	 * @since 4.1
	 */
	List<@Nullable String> jsonMGet(String path, byte[]... keys);

	/**
	 * Increment the number value at the given key and path.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @param number must not be {@literal null}.
	 * @return a list where each element is the new numeric value after incrementing, or {@literal null} if the path does not exist.
	 * @see <a href="https://redis.io/docs/latest/commands/json.numincrby/">Redis Documentation: JSON.NUMINCRBY</a>
	 * @since 4.1
	 */
	List<@Nullable Number> jsonNumIncrBy(byte[] key, String path, Number number);

	/**
	 * Set the JSON value at the root path of the given key.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return {@literal true} if the key was set, {@literal false} otherwise.
	 * @see <a href="https://redis.io/docs/latest/commands/json.set/">Redis Documentation: JSON.SET</a>
	 * @since 4.1
	 */
	default Boolean jsonSet(byte[] key, String value) {
		return jsonSet(key, ROOT_PATH, value, JsonSetOption.upsert());
	}

	/**
	 * Set the JSON value at the given key and path.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @param option must not be {@literal null}.
	 * @return {@literal true} if the key was set, {@literal false} otherwise.
	 * @see <a href="https://redis.io/docs/latest/commands/json.set/">Redis Documentation: JSON.SET</a>
	 * @since 4.1
	 */
	Boolean jsonSet(byte[] key, String path, String value, JsonSetOption option);

	/**
	 * Append a string value to the JSON string at the given path.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @param value must not be {@literal null}. Value must be JSON encoded.
	 * @return a list where each element is the new string length or {@literal null} if path does not exist.
	 * @see <a href="https://redis.io/docs/latest/commands/json.strappend/">Redis Documentation: JSON.STRAPPEND</a>
	 * @since 4.1
	 */
	List<@Nullable Long> jsonStrAppend(byte[] key, String path, String value);

	/**
	 * Get the length of the JSON string value at the given path.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @return a list where each element is the string length or {@literal null} if path does not exist.
	 * @see <a href="https://redis.io/docs/latest/commands/json.strlen/">Redis Documentation: JSON.STRLEN</a>
	 * @since 4.1
	 */
	List<@Nullable Long> jsonStrLen(byte[] key, String path);

	/**
	 * Toggle boolean values at the given key and path.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @return a list where each element is the new boolean value after toggling, or {@literal null} if the path does not exist.
	 * @see <a href="https://redis.io/docs/latest/commands/json.toggle/">Redis Documentation: JSON.TOGGLE</a>
	 * @since 4.1
	 */
	List<@Nullable Boolean> jsonToggle(byte[] key, String path);

	/**
	 * Get the JSON type at the root path of the given key.
	 *
	 * @param key must not be {@literal null}.
	 * @return a list where each element is the type at the given path.
	 * @see <a href="https://redis.io/docs/latest/commands/json.type/">Redis Documentation: JSON.TYPE</a>
	 * @since 4.1
	 */
	default List<@Nullable JsonType> jsonType(byte[] key) {
		return jsonType(key, ROOT_PATH);
	}

	/**
	 * Get the JSON type at the given key and path.
	 *
	 * @param key must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @return a list where each element is the type at the given path.
	 * @see <a href="https://redis.io/docs/latest/commands/json.type/">Redis Documentation: JSON.TYPE</a>
	 * @since 4.1
	 */
	List<@Nullable JsonType> jsonType(byte[] key, String path);

	/**
	 * {@code JSON.SET} command arguments for {@code NX}, {@code XX}.
	 */
	enum JsonSetOption {

		/**
		 * Do not set any additional command argument.
		 */
		UPSERT,

		/**
		 * {@code NX}
		 */
		IF_PATH_NOT_EXISTS,

		/**
		 * {@code XX}
		 */
		IF_PATH_EXISTS;

		/**
		 * Do not set any additional command argument.
		 *
		 * @return {@link JsonSetOption#UPSERT}
		 */
		public static JsonSetOption upsert() {
			return UPSERT;
		}

		/**
		 * {@code NX}
		 *
		 * @return {@link JsonSetOption#IF_PATH_NOT_EXISTS}
		 */
		public static JsonSetOption ifPathNotExists() {
			return IF_PATH_NOT_EXISTS;
		}

		/**
		 * {@code XX}
		 *
		 * @return {@link JsonSetOption#IF_PATH_EXISTS}
		 */
		public static JsonSetOption ifPathExists() {
			return IF_PATH_EXISTS;
		}

	}

	enum JsonType {

		STRING,
		NUMBER,
		BOOLEAN,
		OBJECT,
		ARRAY,
		UNKNOWN

	}

}
