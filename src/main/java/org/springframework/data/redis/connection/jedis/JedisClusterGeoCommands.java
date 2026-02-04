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

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;

import org.springframework.data.redis.connection.RedisGeoCommands;

/**
 * Cluster {@link RedisGeoCommands} implementation for Jedis.
 * <p>
 * This class can be used to override only methods that require cluster-specific handling.
 * <p>
 * Pipeline and transaction modes are not supported in cluster mode.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Tihomir Mateev
 * @since 2.0
 */
@NullUnmarked
class JedisClusterGeoCommands extends JedisGeoCommands {

	JedisClusterGeoCommands(@NonNull JedisClusterConnection connection) {
		super(connection);
	}
}
