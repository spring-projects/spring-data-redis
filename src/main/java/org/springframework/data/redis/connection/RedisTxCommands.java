/*
 * Copyright 2011-2018 the original author or authors.
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

/**
 * Transaction/Batch specific commands supported by Redis.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public interface RedisTxCommands {

	/**
	 * Mark the start of a transaction block. <br>
	 * Commands will be queued and can then be executed by calling {@link #exec()} or rolled back using {@link #discard()}
	 * <p>
	 *
	 * @see <a href="http://redis.io/commands/multi">Redis Documentation: MULTI</a>
	 */
	void multi();

	/**
	 * Executes all queued commands in a transaction started with {@link #multi()}. <br>
	 * If used along with {@link #watch(byte[]...)} the operation will fail if any of watched keys has been modified.
	 *
	 * @return List of replies for each executed command.
	 * @see <a href="http://redis.io/commands/exec">Redis Documentation: EXEC</a>
	 */
	List<Object> exec();

	/**
	 * Discard all commands issued after {@link #multi()}.
	 *
	 * @see <a href="http://redis.io/commands/discard">Redis Documentation: DISCARD</a>
	 */
	void discard();

	/**
	 * Watch given {@code keys} for modifications during transaction started with {@link #multi()}.
	 *
	 * @param keys must not be {@literal null}.
	 * @see <a href="http://redis.io/commands/watch">Redis Documentation: WATCH</a>
	 */
	void watch(byte[]... keys);

	/**
	 * Flushes all the previously {@link #watch(byte[]...)} keys.
	 *
	 * @see <a href="http://redis.io/commands/unwatch">Redis Documentation: UNWATCH</a>
	 */
	void unwatch();
}
