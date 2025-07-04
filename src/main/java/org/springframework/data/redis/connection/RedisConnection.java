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

import java.util.List;

import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;
import org.springframework.dao.DataAccessException;

/**
 * A connection to a Redis server.
 * <p>
 * The {@link RedisConnection} interface serves as a common abstraction across various Redis client libraries (or
 * drivers).
 * <p>
 * Additionally, performs exception translation between the underlying Redis client library and Spring DAO exceptions.
 * The methods follow as much as possible the Redis names and conventions.
 * <p>
 * {@link RedisConnection Redis connections}, unlike perhaps their underlying native connection are not Thread-safe and
 * should not be shared across multiple threads, concurrently or simultaneously.
 * <p>
 * Redis command methods are exempted from the default {@literal non-nullable} return value assumption as nullness
 * depends not only on the command but also on the connection state. Methods invoked during a transaction or while
 * pipelining are required to return {@literal null} at the time invoking a command as the response is not available
 * until the transaction is executed or the pipeline is closed. To avoid excessive null checks in calling code and to
 * not express a faulty assumption of non-nullness, all command interfaces are annotated with {@code @NullUnmarked}.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author James Howe
 * @author John Blum
 */
@NullUnmarked
public interface RedisConnection extends RedisCommandsProvider, DefaultedRedisConnection, AutoCloseable {

	/**
	 * Closes or quits the connection.
	 *
	 * @throws DataAccessException if the {@link RedisConnection} could not be closed.
	 */
	@Override
	void close() throws DataAccessException;

	/**
	 * Indicates whether the underlying connection is closed or not.
	 *
	 * @return true if the connection is closed, false otherwise.
	 */
	boolean isClosed();

	/**
	 * Returns the native connection (the underlying library/driver object).
	 *
	 * @return underlying, native object
	 */
	Object getNativeConnection();

	/**
	 * Indicates whether the connection is in "queue"(or "MULTI") mode or not. When queueing, all commands are postponed
	 * until EXEC or DISCARD commands are issued. Since in queueing no results are returned, the connection will return
	 * NULL on all operations that interact with the data.
	 *
	 * @return true if the connection is in queue/MULTI mode, false otherwise
	 */
	boolean isQueueing();

	/**
	 * Indicates whether the connection is currently pipelined or not.
	 *
	 * @return true if the connection is pipelined, false otherwise
	 * @see #openPipeline()
	 * @see #isQueueing()
	 */
	boolean isPipelined();

	/**
	 * Activates the pipeline mode for this connection. When pipelined, all commands return null (the reply is read at the
	 * end through {@link #closePipeline()}. Calling this method when the connection is already pipelined has no effect.
	 * Pipelining is used for issuing commands without requesting the response right away but rather at the end of the
	 * batch. While somewhat similar to MULTI, pipelining does not guarantee atomicity - it only tries to improve
	 * performance when issuing a lot of commands (such as in batching scenarios).
	 * <p>
	 * Note:
	 * </p>
	 * Consider doing some performance testing before using this feature since in many cases the performance benefits are
	 * minimal yet the impact on usage are not.
	 *
	 * @see #multi()
	 */
	void openPipeline();

	/**
	 * Executes the commands in the pipeline and returns their result. If the connection is not pipelined, an empty
	 * collection is returned.
	 *
	 * @throws RedisPipelineException if the pipeline contains any incorrect/invalid statements
	 * @return the result of the executed commands.
	 */
	List<@Nullable Object> closePipeline() throws RedisPipelineException;

	/**
	 * @return the {@link RedisSentinelConnection} when using Redis Sentinel.
	 * @since 1.4
	 */
	RedisSentinelConnection getSentinelConnection();

}
