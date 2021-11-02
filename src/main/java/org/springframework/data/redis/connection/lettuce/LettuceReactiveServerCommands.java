/*
 * Copyright 2017-2022 the original author or authors.
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
package org.springframework.data.redis.connection.lettuce;

import io.lettuce.core.api.reactive.RedisServerReactiveCommands;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.connection.ReactiveServerCommands;
import org.springframework.data.redis.connection.RedisServerCommands.FlushOption;
import org.springframework.data.redis.core.types.RedisClientInfo;
import org.springframework.data.redis.util.ByteUtils;
import org.springframework.util.Assert;

/**
 * {@link ReactiveServerCommands} implementation for {@literal Lettuce}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Dennis Neufeld
 */
class LettuceReactiveServerCommands implements ReactiveServerCommands {

	private final LettuceReactiveRedisConnection connection;

	/**
	 * Create new {@link LettuceReactiveGeoCommands}.
	 *
	 * @param connection must not be {@literal null}.
	 * @throws IllegalArgumentException when {@code connection} is {@literal null}.
	 */
	LettuceReactiveServerCommands(LettuceReactiveRedisConnection connection) {

		Assert.notNull(connection, "Connection must not be null!");

		this.connection = connection;
	}

	@Override
	public Mono<String> bgReWriteAof() {
		return connection.execute(RedisServerReactiveCommands::bgrewriteaof).next();
	}

	@Override
	public Mono<String> bgSave() {
		return connection.execute(RedisServerReactiveCommands::bgsave).next();
	}

	@Override
	public Mono<Long> lastSave() {
		return connection.execute(RedisServerReactiveCommands::lastsave).next().map(Date::getTime);
	}

	@Override
	public Mono<String> save() {
		return connection.execute(RedisServerReactiveCommands::save).next();
	}

	@Override
	public Mono<Long> dbSize() {
		return connection.execute(RedisServerReactiveCommands::dbsize).next();
	}

	@Override
	public Mono<String> flushDb() {
		return connection.execute(RedisServerReactiveCommands::flushdb).next();
	}

	@Override
	public Mono<String> flushDb(FlushOption option) {
		return connection.execute(it -> it.flushdb(LettuceServerCommands.toFlushMode(option))).next();
	}

	@Override
	public Mono<String> flushAll() {
		return connection.execute(RedisServerReactiveCommands::flushall).next();
	}

	@Override
	public Mono<String> flushAll(FlushOption option) {
		return connection.execute(it -> it.flushall(LettuceServerCommands.toFlushMode(option))).next();
	}

	@Override
	public Mono<Properties> info() {

		return connection.execute(RedisServerReactiveCommands::info) //
				.map(LettuceConverters::toProperties) //
				.next();
	}

	@Override
	public Mono<Properties> info(String section) {

		Assert.hasText(section, "Section must not be null or empty!");

		return connection.execute(c -> c.info(section)) //
				.map(LettuceConverters::toProperties) //
				.next();
	}

	@Override
	public Mono<Properties> getConfig(String pattern) {

		Assert.hasText(pattern, "Pattern must not be null or empty!");

		return connection.execute(c -> c.configGet(pattern)) //
				.map(LettuceConverters::toProperties).next();
	}

	@Override
	public Mono<String> setConfig(String param, String value) {

		Assert.hasText(param, "Parameter must not be null or empty!");
		Assert.hasText(value, "Value must not be null or empty!");

		return connection.execute(c -> c.configSet(param, value)).next();
	}

	@Override
	public Mono<String> resetConfigStats() {
		return connection.execute(RedisServerReactiveCommands::configResetstat).next();
	}

	@Override
	public Mono<Long> time(TimeUnit timeUnit) {

		return connection.execute(RedisServerReactiveCommands::time) //
				.map(ByteUtils::getBytes) //
				.collectList() //
				.map(LettuceConverters.toTimeConverter(timeUnit)::convert);
	}

	@Override
	public Mono<String> killClient(String host, int port) {

		Assert.notNull(host, "Host must not be null or empty!");

		return connection.execute(c -> c.clientKill(String.format("%s:%s", host, port))).next();
	}

	@Override
	public Mono<String> setClientName(String name) {

		Assert.hasText(name, "Name must not be null or empty!");

		return connection.execute(c -> c.clientSetname(ByteBuffer.wrap(LettuceConverters.toBytes(name)))).next();
	}

	@Override
	public Mono<String> getClientName() {

		return connection.execute(RedisServerReactiveCommands::clientGetname) //
				.map(ByteUtils::getBytes) //
				.map(LettuceConverters::toString) //
				.next();
	}

	@Override
	public Flux<RedisClientInfo> getClientList() {

		return connection.execute(RedisServerReactiveCommands::clientList)
				.concatMapIterable(s -> LettuceConverters.stringToRedisClientListConverter().convert(s));
	}
}
