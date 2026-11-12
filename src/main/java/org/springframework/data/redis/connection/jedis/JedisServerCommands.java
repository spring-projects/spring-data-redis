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

import redis.clients.jedis.Protocol;
import redis.clients.jedis.UnifiedJedis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
 * @author WooSeung Kim
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
		connection.invoke().just(Protocol.Command.BGREWRITEAOF);
	}

	@Override
	public void bgSave() {
		connection.invoke().just(Protocol.Command.BGSAVE);
	}

	@Override
	public Long lastSave() {
		return connection.invoke().from(Protocol.Command.LASTSAVE)
				.get(response -> (Long) response);
	}

	@Override
	public void save() {
		connection.invokeStatus().just(Protocol.Command.SAVE);
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
		connection.invokeStatus().just(Protocol.Command.FLUSHDB,
				cmd -> cmd.add(JedisConverters.toFlushMode(option).name()));
	}

	@Override
	public void flushAll() {
		connection.invokeStatus().just(UnifiedJedis::flushAll);
	}

	@Override
	public void flushAll(@NonNull FlushOption option) {
		connection.invokeStatus().just(Protocol.Command.FLUSHALL,
				cmd -> cmd.add(JedisConverters.toFlushMode(option).name()));
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
		connection.invokeStatus().just(Protocol.Command.SHUTDOWN);
	}

	@Override
	public void shutdown(@Nullable ShutdownOption option) {

		if (option == null) {
			shutdown();
			return;
		}

		String saveOption = (option == ShutdownOption.NOSAVE) ? "NOSAVE" : "SAVE";
		connection.invokeStatus()
				.just(Protocol.Command.SHUTDOWN, cmd -> cmd.add(saveOption));
	}

	@Override
	@SuppressWarnings("unchecked")
	public Properties getConfig(@NonNull String pattern) {

		Assert.notNull(pattern, "Pattern must not be null");

		return connection.invoke()
				.from(Protocol.Command.CONFIG, cmd -> cmd.add("GET").add(pattern))
				.get(response -> {
					List<Object> list = (List<Object>) response;
					Properties props = new Properties();

					if (!list.isEmpty()) {
						Object item = list.get(0);
						if (item instanceof Map.Entry) {
							for (Object o : list) {
								Map.Entry<byte[], byte[]> entry = (Map.Entry<byte[], byte[]>) o;
								props.setProperty(new String(entry.getKey()), new String(entry.getValue()));
							}
							return props;
						}
					}

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
		connection.invokeStatus()
				.just(Protocol.Command.CONFIG, cmd -> cmd.add("RESETSTAT"));
	}

	@Override
	public void rewriteConfig() {
		connection.invokeStatus().just(Protocol.Command.CONFIG, cmd -> cmd.add("REWRITE"));
	}

	@Override
	@SuppressWarnings("unchecked")
	public Long time(@NonNull TimeUnit timeUnit) {

		Assert.notNull(timeUnit, "TimeUnit must not be null");

		return connection.invoke().from(Protocol.Command.TIME)
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

		connection.invokeStatus().just(Protocol.Command.CLIENT, cmd -> cmd.add("KILL").add("%s:%s".formatted(host, port)));
	}

	@Override
	public void setClientName(byte @NonNull [] name) {

		Assert.notNull(name, "Name must not be null");

		connection.invokeStatus()
				.just(Protocol.Command.CLIENT, cmd -> cmd.add("SETNAME".getBytes()).add(name));
	}

	@Override
	public String getClientName() {
		return connection.invokeStatus()
				.from(Protocol.Command.CLIENT, cmd -> cmd.add("GETNAME"))
				.get(response -> new String((byte[]) response));
	}

	@Override
	public List<@NonNull RedisClientInfo> getClientList() {
		return connection.invokeStatus()
				.from(Protocol.Command.CLIENT, cmd -> cmd.add("LIST"))
				.get(response -> JedisConverters.toListOfRedisClientInformation(new String((byte[]) response)));
	}

	@Override
	public void replicaOf(@NonNull String host, int port) {

		Assert.hasText(host, "Host must not be null for 'REPLICAOF' command");

		connection.invokeStatus().just(
				Protocol.Command.REPLICAOF, cmd -> cmd.add(host).add(String.valueOf(port)));
	}

	@Override
	public void replicaOfNoOne() {
		connection.invokeStatus()
				.just(Protocol.Command.REPLICAOF, cmd -> cmd.add("NO").add("ONE"));

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

		// Clamp instead of rejecting: the no-timeout overload passes Long.MAX_VALUE as "wait forever" sentinel
		int timeoutToUse = timeout <= Integer.MAX_VALUE ? (int) timeout : Integer.MAX_VALUE;

		// UnifiedJedis.migrate(host, port, timeout, params, keys) hardcodes destination-db to 0.
		connection.invokeStatus().just(Protocol.Command.MIGRATE, cmd -> {
			cmd.add(target.getRequiredHost()).add(target.getRequiredPort()).add(new byte[0]).add(dbIndex).add(timeoutToUse);
			if (option == MigrateOption.COPY) {
				cmd.add(Protocol.Keyword.COPY);
			} else if (option == MigrateOption.REPLACE) {
				cmd.add(Protocol.Keyword.REPLACE);
			}
			cmd.add(Protocol.Keyword.KEYS).add(key);
		});
	}

}
