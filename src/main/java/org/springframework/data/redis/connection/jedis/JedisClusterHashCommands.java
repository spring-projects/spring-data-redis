/*
 * Copyright 2017-present the original author or authors.
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
package org.springframework.data.redis.connection.jedis;

import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.Map.Entry;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;

import org.springframework.data.redis.connection.RedisHashCommands;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanCursor;
import org.springframework.data.redis.core.ScanIteration;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.util.Assert;

/**
 * Cluster {@link RedisHashCommands} implementation for Jedis.
 * <p>
 * <p>
 * This class can be used to override only methods that require cluster-specific handling.
 * <p>
 * Pipeline and transaction modes are not supported in cluster mode.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author John Blum
 * @author Tihomir Mateev
 * @since 2.0
 */
@NullUnmarked
class JedisClusterHashCommands extends JedisHashCommands {

	JedisClusterHashCommands(JedisClusterConnection connection) {
		super(connection);
	}

	@Override
	public Cursor<@NonNull Entry<byte @NonNull [], byte @NonNull []>> hScan(byte @NonNull [] key,
			@NonNull ScanOptions options) {

		Assert.notNull(key, "Key must not be null");

		return new ScanCursor<Entry<byte[], byte[]>>(options) {

			@Override
			protected ScanIteration<Entry<byte[], byte[]>> doScan(CursorId cursorId, ScanOptions options) {

				ScanParams params = JedisConverters.toScanParams(options);

				ScanResult<Entry<byte[], byte[]>> result = getConnection().getJedis().hscan(key,
						JedisConverters.toBytes(cursorId), params);
				return new ScanIteration<>(CursorId.of(result.getCursor()), result.getResult());
			}
		}.open();
	}
}
