/*
 * Copyright 2011-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.connection;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;

/**
 * Hash-specific commands supported by Redis.
 * 
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public interface RedisHashCommands {

	/**
	 * Set the {@code value} of a hash {@code field}.
	 * 
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @param value
	 * @return
	 * @see <a href="http://redis.io/commands/hset">Redis Documentation: HSET</a>
	 */
	Boolean hSet(byte[] key, byte[] field, byte[] value);

	/**
	 * Set the {@code value} of a hash {@code field} only if {@code field} does not exist.
	 * 
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @param value
	 * @return
	 * @see <a href="http://redis.io/commands/hsetnx">Redis Documentation: HSETNX</a>
	 */
	Boolean hSetNX(byte[] key, byte[] field, byte[] value);

	/**
	 * Get value for given {@code field} from hash at {@code key}.
	 * 
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/hget">Redis Documentation: HGET</a>
	 */
	byte[] hGet(byte[] key, byte[] field);

	/**
	 * Get values for given {@code fields} from hash at {@code key}.
	 * 
	 * @param key must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/hmget">Redis Documentation: HMGET</a>
	 */
	List<byte[]> hMGet(byte[] key, byte[]... fields);

	/**
	 * Set multiple hash fields to multiple values using data provided in {@code hashes}
	 * 
	 * @param key must not be {@literal null}.
	 * @param hashes must not be {@literal null}.
	 * @see <a href="http://redis.io/commands/hmset">Redis Documentation: HMSET</a>
	 */
	void hMSet(byte[] key, Map<byte[], byte[]> hashes);

	/**
	 * Increment {@code value} of a hash {@code field} by the given {@code delta}.
	 * 
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @param delta
	 * @return
	 * @see <a href="http://redis.io/commands/hincrby">Redis Documentation: HINCRBY</a>
	 */
	Long hIncrBy(byte[] key, byte[] field, long delta);

	/**
	 * Increment {@code value} of a hash {@code field} by the given {@code delta}.
	 * 
	 * @param key must not be {@literal null}.
	 * @param field
	 * @param delta
	 * @return
	 * @see <a href="http://redis.io/commands/hincrbyfloat">Redis Documentation: HINCRBYFLOAT</a>
	 */
	Double hIncrBy(byte[] key, byte[] field, double delta);

	/**
	 * Determine if given hash {@code field} exists.
	 * 
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/hexits">Redis Documentation: HEXISTS</a>
	 */
	Boolean hExists(byte[] key, byte[] field);

	/**
	 * Delete given hash {@code fields}.
	 *
	 * @param key must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/hdel">Redis Documentation: HDEL</a>
	 */
	Long hDel(byte[] key, byte[]... fields);

	/**
	 * Get size of hash at {@code key}.
	 * 
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/hlen">Redis Documentation: HLEN</a>
	 */
	Long hLen(byte[] key);

	/**
	 * Get key set (fields) of hash at {@code key}.
	 * 
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/hkeys">Redis Documentation: HKEYS</a>?
	 */
	Set<byte[]> hKeys(byte[] key);

	/**
	 * Get entry set (values) of hash at {@code field}.
	 * 
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/hvals">Redis Documentation: HVALS</a>
	 */
	List<byte[]> hVals(byte[] key);

	/**
	 * Get entire hash stored at {@code key}.
	 * 
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/hgetall">Redis Documentation: HGETALL</a>
	 */
	Map<byte[], byte[]> hGetAll(byte[] key);

	/**
	 * Use a {@link Cursor} to iterate over entries in hash at {@code key}.
	 * 
	 * @param key must not be {@literal null}.
	 * @param options must not be {@literal null}.
	 * @return
	 * @since 1.4
	 * @see <a href="http://redis.io/commands/hscan">Redis Documentation: HSCAN</a>
	 */
	Cursor<Map.Entry<byte[], byte[]>> hScan(byte[] key, ScanOptions options);
}
