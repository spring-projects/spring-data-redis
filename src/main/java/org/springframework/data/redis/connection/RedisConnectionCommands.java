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

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;

/**
 * Connection-specific commands supported by Redis.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@NullUnmarked
public interface RedisConnectionCommands {

	/**
	 * Select the DB with given positive {@code dbIndex}.
	 *
	 * @param dbIndex the database index.
	 * @see <a href="https://redis.io/commands/select">Redis Documentation: SELECT</a>
	 */
	void select(int dbIndex);

	/**
	 * Returns {@code message} via server roundtrip.
	 *
	 * @param message the message to echo.
	 * @return the message or {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/echo">Redis Documentation: ECHO</a>
	 */
	byte[] echo(byte @NonNull [] message);

	/**
	 * Test connection.
	 *
	 * @return Server response message - usually {@literal PONG}. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/ping">Redis Documentation: PING</a>
	 */
	String ping();
}
