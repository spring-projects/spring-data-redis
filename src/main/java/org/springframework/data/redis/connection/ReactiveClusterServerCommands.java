/*
 * Copyright 2017 the original author or authors.
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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Properties;

import org.springframework.data.redis.core.types.RedisClientInfo;

/**
 * @author Mark Paluch
 * @since 2.0
 */
public interface ReactiveClusterServerCommands extends ReactiveServerCommands {

	/**
	 * @param node must not be {@literal null}.
	 * @see RedisServerCommands#bgReWriteAof()
	 */
	Mono<String> bgReWriteAof(RedisClusterNode node);

	/**
	 * @param node must not be {@literal null}.
	 * @see RedisServerCommands#bgSave()
	 */
	Mono<String> bgSave(RedisClusterNode node);

	/**
	 * @param node must not be {@literal null}.
	 * @return
	 * @see RedisServerCommands#lastSave()
	 */
	Mono<Long> lastSave(RedisClusterNode node);

	/**
	 * @param node must not be {@literal null}.
	 * @see RedisServerCommands#save()
	 */
	Mono<String> save(RedisClusterNode node);

	/**
	 * @param node must not be {@literal null}.
	 * @return
	 * @see RedisServerCommands#dbSize()
	 */
	Mono<Long> dbSize(RedisClusterNode node);

	/**
	 * @param node must not be {@literal null}.
	 * @see RedisServerCommands#flushDb()
	 */
	Mono<String> flushDb(RedisClusterNode node);

	/**
	 * @param node must not be {@literal null}.
	 * @see RedisServerCommands#flushAll()
	 */
	Mono<String> flushAll(RedisClusterNode node);

	/**
	 * @param node must not be {@literal null}.
	 * @return
	 * @see RedisServerCommands#info()
	 */
	Mono<Properties> info(RedisClusterNode node);

	/**
	 * @param node must not be {@literal null}.
	 * @param section
	 * @return
	 * @see RedisServerCommands#info(String)
	 */
	Mono<Properties> info(RedisClusterNode node, String section);

	/**
	 * @param node must not be {@literal null}.
	 * @param pattern
	 * @return
	 * @see RedisServerCommands#getConfig(String)
	 */
	Mono<Properties> getConfig(RedisClusterNode node, String pattern);

	/**
	 * @param node must not be {@literal null}.
	 * @param param
	 * @param value
	 * @see RedisServerCommands#setConfig(String, String)
	 */
	Mono<String> setConfig(RedisClusterNode node, String param, String value);

	/**
	 * @param node must not be {@literal null}.
	 * @see RedisServerCommands#resetConfigStats()
	 */
	Mono<String> resetConfigStats(RedisClusterNode node);

	/**
	 * @param node must not be {@literal null}.
	 * @return
	 * @see RedisServerCommands#time()
	 */
	Mono<Long> time(RedisClusterNode node);

	/**
	 * @param node must not be {@literal null}.
	 * @return
	 * @see RedisServerCommands#getClientList()
	 */
	Flux<RedisClientInfo> getClientList(RedisClusterNode node);
}
