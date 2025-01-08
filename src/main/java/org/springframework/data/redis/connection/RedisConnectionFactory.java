/*
 * Copyright 2011-2025 the original author or authors.
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

import org.springframework.dao.support.PersistenceExceptionTranslator;

/**
 * Thread-safe factory of Redis connections.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author John Blum
 */
public interface RedisConnectionFactory extends PersistenceExceptionTranslator {

	/**
	 * Specifies if pipelined results should be converted to the expected data type.
	 * <p>
	 * If {@literal false}, results of {@link RedisConnection#closePipeline()} and {@link RedisConnection#exec()} will be
	 * of the type returned by the underlying driver. This method is mostly for backwards compatibility with
	 * {@literal 1.0}. It is generally always a good idea to allow results to be converted and deserialized. In fact, this
	 * is now the default behavior.
	 *
	 * @return {@code true} to convert pipeline and transaction results; {@code false} otherwise.
	 */
	boolean getConvertPipelineAndTxResults();

	/**
	 * Returns a suitable {@link RedisConnection connection} for interacting with Redis.
	 *
	 * @return {@link RedisConnection connection} for interacting with Redis.
	 * @throws IllegalStateException if the connection factory requires initialization and the factory has not yet been
	 *           initialized.
	 */
	RedisConnection getConnection();

	/**
	 * Returns a suitable {@link RedisClusterConnection connection} for interacting with Redis Cluster.
	 *
	 * @return a {@link RedisClusterConnection connection} for interacting with Redis Cluster.
	 * @throws IllegalStateException if the connection factory requires initialization and the factory has not yet been
	 *           initialized.
	 * @since 1.7
	 */
	RedisClusterConnection getClusterConnection();

	/**
	 * Returns a suitable {@link RedisSentinelConnection connection} for interacting with Redis Sentinel.
	 *
	 * @return a {@link RedisSentinelConnection connection} for interacting with Redis Sentinel.
	 * @throws IllegalStateException if the connection factory requires initialization and the factory has not yet been
	 *           initialized.
	 * @since 1.4
	 */
	RedisSentinelConnection getSentinelConnection();

}
