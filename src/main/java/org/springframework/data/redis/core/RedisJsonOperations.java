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

import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.serializer.RedisJsonSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;

/**
 * Interface that specifies Redis JSON operations.
 * <p>
 * Implemented by {@link RedisJsonTemplate}. Not often used directly, but a useful option to enhance testability, as it
 * can easily be mocked or stubbed.
 * <p>
 * Specification objects are immutable and can be used to build up complex queries.
 * <p>
 * JSON path expressions follow the
 * <a href="https://redis.io/docs/latest/develop/data-types/json/path/#jsonpath-syntax">RedisJSON</a> version 2 path
 * syntax. Unless specified otherwise, results are positionally correlated to matching paths: each element in the
 * returned {@link List} corresponds to a matching path, with {@literal null} indicating that the matched value has an
 * incompatible JSON type.
 *
 * @author Yordan Tsintsov
 * @author Mark Paluch
 * @since 4.2
 * @see RedisJsonTemplate
 * @see org.springframework.data.redis.connection.json.JsonPath
 * @see org.springframework.data.redis.connection.json.JsonValue
 * @param <K> the Redis key type.
 */
public interface RedisJsonOperations<K> extends JsonOperations<K> {

	/**
	 * Delete given {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal true} if the key was removed.
	 * @see <a href="https://redis.io/commands/del">Redis Documentation: DEL</a>
	 */
	default Boolean delete(K key) {
		return key(key).delete();
	}

	/**
	 * Start building a key-bound operation for the given {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return a spec for specifying the key-bound operation.
	 */
	KeySpec key(K key);

	/**
	 * Start building a key-bound operation for the given {@code keys}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return a spec for specifying the key-bound operation.
	 */
	default KeysSpec keys(K... keys) {
		return keys(List.of(keys));
	}

	/**
	 * Start building a key-bound operation for the given {@code keys}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return a spec for specifying the key-bound operation.
	 */
	KeysSpec keys(Collection<K> keys);

	/**
	 * Create a new {@link RedisJsonOperations} using the given {@link RedisConnectionFactory} and default serializers for
	 * usage with {@link String} keys.
	 *
	 * @param connectionFactory the connection factory to use.
	 * @return a new {@link RedisJsonOperations} instance.
	 * @see RedisJsonTemplate#(RedisConnectionFactory, RedisSerializer, RedisJsonSerializer)
	 */
	static RedisJsonOperations<String> create(RedisConnectionFactory connectionFactory) {
		return RedisJsonTemplate.create(connectionFactory);
	}

	/**
	 * Specification for key-bound operations for a single key.
	 */
	interface KeySpec extends KeyDeleteSpec {

	}

	/**
	 * Delete operations for a single key.
	 */
	interface KeyDeleteSpec {

		/**
		 * Delete the previously configured key from the keyspace.
		 *
		 * @return {@literal true} if the key was removed; {@literal false} otherwise.
		 * @see <a href="https://redis.io/commands/del">Redis Documentation: DEL</a>
		 */

		Boolean delete();

		/**
		 * Unlink the previously configured key from the keyspace. Unlike with {@link #delete(Object)} the actual memory
		 * reclaiming here happens asynchronously.
		 *
		 * @return {@literal true} if the key was removed; {@literal false} otherwise.
		 * @see <a href="https://redis.io/commands/unlink">Redis Documentation: UNLINK</a>
		 */

		Boolean unlink();

	}

	/**
	 * Specification for key-bound operations for one or many keys.
	 */
	interface KeysSpec extends KeysDeleteSpec {

	}

	/**
	 * Delete operations for one or many keys.
	 */
	interface KeysDeleteSpec {

		/**
		 * Delete the previously configured keys from the keyspace.
		 *
		 * @return the number of keys that were removed.
		 * @see <a href="https://redis.io/commands/del">Redis Documentation: DEL</a>
		 */

		Long delete();

		/**
		 * Unlink the previously configured keys from the keyspace. Unlike with {@link #delete(Object)} the actual memory
		 * reclaiming here happens asynchronously.
		 *
		 * @return the number of keys that were removed.
		 * @see <a href="https://redis.io/commands/unlink">Redis Documentation: UNLINK</a>
		 */

		Long unlink();

	}

}
