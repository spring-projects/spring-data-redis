/*
 * Copyright 2014-present the original author or authors.
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

import java.util.Collection;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;

/**
 * Redis Sentinel-specific commands.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.4
 * @see <a href="https://redis.io/topics/sentinel">Redis Sentinel Documentation</a>
 */
@NullUnmarked
public interface RedisSentinelCommands {

	/**
	 * Force a failover as if the {@literal master} was not reachable.
	 *
	 * @param master must not be {@literal null}.
	 */
	void failover(@NonNull NamedNode master);

	/**
	 * Get a {@link Collection} of monitored masters and their state.
	 *
	 * @return Collection of {@link RedisServer}s. Never {@literal null}.
	 */
	Collection<@NonNull RedisServer> masters();

	/**
	 * Show list of replicas for given {@literal master}.
	 *
	 * @param master must not be {@literal null}.
	 * @return Collection of {@link RedisServer}s. Never {@literal null}.
	 */
	Collection<@NonNull RedisServer> replicas(@NonNull NamedNode master);

	/**
	 * Removes given {@literal master}. The server will no longer be monitored and will no longer be returned by
	 * {@link #masters()}.
	 *
	 * @param master must not be {@literal null}.
	 */
	void remove(@NonNull NamedNode master);

	/**
	 * Tell sentinel to start monitoring a new {@literal master} with the specified {@link RedisServer#getName()},
	 * {@link RedisServer#getHost()}, {@link RedisServer#getPort()}, and {@link RedisServer#getQuorum()}.
	 *
	 * @param master must not be {@literal null}.
	 */
	void monitor(@NonNull RedisServer master);

}
