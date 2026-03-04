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

import redis.clients.jedis.*;
import redis.clients.jedis.params.MigrateParams;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.RedisServerCommands;
import org.springframework.data.redis.core.types.RedisClientInfo;
import org.springframework.util.Assert;

/**
 * @author Mark Paluch
 * @author Dennis Neufeld
 * @since 2.0
 */
@NullUnmarked
class JedisServerCommands implements RedisServerCommands {

	private final JedisConnection connection;

	JedisServerCommands(@NonNull JedisConnection connection) {
		this.connection = connection;
	}

	@Override
	public void bgReWriteAof() {
		connection.invoke().just(j -> j.sendCommand(Protocol.Command.BGREWRITEAOF));
	}

	@Override
	public void bgSave() {
		connection.invoke().just(j -> j.sendCommand(Protocol.Command.BGSAVE));
	}

	@Override
	public Long lastSave() {
		return connection.invoke().from(j -> j.sendCommand(Protocol.Command.LASTSAVE))
				.get(response -> (Long) response);
	}

	@Override
	public void save() {
		connection.invokeStatus().just(j -> j.sendCommand(Protocol.Command.SAVE));
	}

	@Override
	public Long dbSize() {
		return connection.invoke().just(UnifiedJedis::dbSize);
	}

	@Override
	public void flushDb() {
		connection.invokeStatus().just(UnifiedJedis::flushDB);
	}

	@Override
	public void flushDb(@NonNull FlushOption option) {
		connection.invokeStatus().just(j -> j.sendCommand(Protocol.Command.FLUSHDB, JedisConverters.toFlushMode(option).name()));
	}

	@Override
	public void flushAll() {
		connection.invokeStatus().just(UnifiedJedis::flushAll);
	}

	@Override
	public void flushAll(@NonNull FlushOption option) {
		connection.invokeStatus().just(j -> j.sendCommand(Protocol.Command.FLUSHALL, JedisConverters.toFlushMode(option).name()));
	}

	@Override
	public Properties info() {
		return connection.invoke().from(UnifiedJedis::info).get(JedisConverters::toProperties);
	}

	@Override
	public Properties info(@NonNull String section) {

		Assert.notNull(section, "Section must not be null");

		return connection.invoke().from(j -> j.info(section)).get(JedisConverters::toProperties);
	}

	@Override
	public void shutdown() {
		connection.invokeStatus().just(j -> j.sendCommand(Protocol.Command.SHUTDOWN));
	}

	@Override
	public void shutdown(@Nullable ShutdownOption option) {

		if (option == null) {
			shutdown();
			return;
		}

		String saveOption = (option == ShutdownOption.NOSAVE) ? "NOSAVE" : "SAVE";
		connection.invokeStatus().just(j -> j.sendCommand(Protocol.Command.SHUTDOWN, saveOption));
	}

	@Override
	@SuppressWarnings("unchecked")
	public Properties getConfig(@NonNull String pattern) {

		Assert.notNull(pattern, "Pattern must not be null");

		return connection.invoke().from(j -> j.sendCommand(Protocol.Command.CONFIG, "GET", pattern))
				.get(response -> {
					List<Object> list = (List<Object>) response;
					Properties props = new Properties();
					for (int i = 0; i < list.size(); i += 2) {
						String key = new String((byte[]) list.get(i));
						String value = new String((byte[]) list.get(i + 1));
						props.setProperty(key, value);
					}
					return props;
				});
	}

	@Override
	public void setConfig(@NonNull String param, @NonNull String value) {

		Assert.notNull(param, "Parameter must not be null");
		Assert.notNull(value, "Value must not be null");

		connection.invokeStatus().just(j -> j.configSet(param, value));
	}

	@Override
	public void resetConfigStats() {
		connection.invokeStatus().just(j -> j.sendCommand(Protocol.Command.CONFIG, "RESETSTAT"));
	}

	@Override
	public void rewriteConfig() {
		connection.invokeStatus().just(j -> j.sendCommand(Protocol.Command.CONFIG, "REWRITE"));
	}

	@Override
	@SuppressWarnings("unchecked")
	public Long time(@NonNull TimeUnit timeUnit) {

		Assert.notNull(timeUnit, "TimeUnit must not be null");

		return connection.invoke().from(j -> j.sendCommand(Protocol.Command.TIME))
				.get(response -> {
					List<Object> list = (List<Object>) response;
					List<String> timeList = new ArrayList<>();
					for (Object item : list) {
						timeList.add(new String((byte[]) item));
					}
					return JedisConverters.toTime(timeList, timeUnit);
				});
	}

	@Override
	public void killClient(@NonNull String host, int port) {

		Assert.hasText(host, "Host for 'CLIENT KILL' must not be 'null' or 'empty'");

		connection.invokeStatus().just(j -> j.sendCommand(Protocol.Command.CLIENT, "KILL", "%s:%s".formatted(host, port)));
	}

	@Override
	public void setClientName(byte @NonNull [] name) {

		Assert.notNull(name, "Name must not be null");

		connection.invokeStatus().just(j -> j.sendCommand(Protocol.Command.CLIENT, "SETNAME".getBytes(), name));
	}

	@Override
	public String getClientName() {
		return connection.invokeStatus().from(j -> j.sendCommand(Protocol.Command.CLIENT, "GETNAME"))
				.get(response -> new String((byte[]) response));
	}

	@Override
	public List<@NonNull RedisClientInfo> getClientList() {
		return connection.invokeStatus().from(j -> j.sendCommand(Protocol.Command.CLIENT, "LIST"))
				.get(response -> JedisConverters.toListOfRedisClientInformation(new String((byte[]) response)));
	}

	@Override
	public void replicaOf(@NonNull String host, int port) {

		Assert.hasText(host, "Host must not be null for 'REPLICAOF' command");

		connection.invokeStatus().just(j -> j.sendCommand(Protocol.Command.REPLICAOF, host, String.valueOf(port)));
	}

	@Override
	public void replicaOfNoOne() {
		connection.invokeStatus().just(j -> j.sendCommand(Protocol.Command.REPLICAOF, "NO", "ONE"));

	}

	@Override
	public void migrate(byte @NonNull [] key, @NonNull RedisNode target, int dbIndex, @Nullable MigrateOption option) {
		migrate(key, target, dbIndex, option, Long.MAX_VALUE);
	}

	@Override
	public void migrate(byte @NonNull [] key, @NonNull RedisNode target, int dbIndex, @Nullable MigrateOption option,
			long timeout) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(target, "Target node must not be null");

		int timeoutToUse = timeout <= Integer.MAX_VALUE ? (int) timeout : Integer.MAX_VALUE;

		MigrateParams params = new MigrateParams();
		if (option == MigrateOption.COPY) {
			params.copy();
		} else if (option == MigrateOption.REPLACE) {
			params.replace();
		}

		connection.invokeStatus()
				.just(j -> j.migrate(target.getRequiredHost(), target.getRequiredPort(), timeoutToUse, params, key));
	}

}
