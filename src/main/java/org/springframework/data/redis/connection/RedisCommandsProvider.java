/*
 * Copyright 2022-present the original author or authors.
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

/**
 * Provides access to {@link RedisCommands} and the segregated command interfaces.
 *
 * @author Mark Paluch
 * @since 3.0
 */
public interface RedisCommandsProvider {

	/**
	 * Get {@link RedisCommands}.
	 *
	 * @return never {@literal null}.
	 * @since 3.0
	 */
	RedisCommands commands();

	/**
	 * Get {@link RedisGeoCommands}.
	 *
	 * @return never {@literal null}.
	 * @since 2.0
	 */
	RedisGeoCommands geoCommands();

	/**
	 * Get {@link RedisHashCommands}.
	 *
	 * @return never {@literal null}.
	 * @since 2.0
	 */
	RedisHashCommands hashCommands();

	/**
	 * Get {@link RedisHyperLogLogCommands}.
	 *
	 * @return never {@literal null}.
	 * @since 2.0
	 */
	RedisHyperLogLogCommands hyperLogLogCommands();

	/**
	 * Get {@link RedisKeyCommands}.
	 *
	 * @return never {@literal null}.
	 * @since 2.0
	 */
	RedisKeyCommands keyCommands();

	/**
	 * Get {@link RedisListCommands}.
	 *
	 * @return never {@literal null}.
	 * @since 2.0
	 */
	RedisListCommands listCommands();

	/**
	 * Get {@link RedisSetCommands}.
	 *
	 * @return never {@literal null}.
	 * @since 2.0
	 */
	RedisSetCommands setCommands();

	/**
	 * Get {@link RedisScriptingCommands}.
	 *
	 * @return never {@literal null}.
	 * @since 2.0
	 */
	RedisScriptingCommands scriptingCommands();

	/**
	 * Get {@link RedisServerCommands}.
	 *
	 * @return never {@literal null}.
	 * @since 2.0
	 */
	RedisServerCommands serverCommands();

	/**
	 * Get {@link RedisStreamCommands}.
	 *
	 * @return never {@literal null}.
	 * @since 2.2
	 */
	RedisStreamCommands streamCommands();

	/**
	 * Get {@link RedisStringCommands}.
	 *
	 * @return never {@literal null}.
	 * @since 2.0
	 */
	RedisStringCommands stringCommands();

	/**
	 * Get {@link RedisZSetCommands}.
	 *
	 * @return never {@literal null}.
	 * @since 2.0
	 */
	RedisZSetCommands zSetCommands();
}
