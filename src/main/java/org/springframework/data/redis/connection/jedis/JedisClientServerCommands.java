/*
 * Copyright 2026-present the original author or authors.
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

import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.args.SaveMode;
import redis.clients.jedis.params.MigrateParams;

import static org.springframework.data.redis.connection.convert.Converters.toProperties;
import static org.springframework.data.redis.connection.jedis.JedisConverters.*;
import static org.springframework.data.redis.connection.jedis.JedisConverters.toBytes;
import static org.springframework.data.redis.connection.jedis.JedisConverters.toTime;
import static redis.clients.jedis.Protocol.Command.*;
import static redis.clients.jedis.Protocol.Keyword.GETNAME;
import static redis.clients.jedis.Protocol.Keyword.KILL;
import static redis.clients.jedis.Protocol.Keyword.LIST;
import static redis.clients.jedis.Protocol.Keyword.NO;
import static redis.clients.jedis.Protocol.Keyword.ONE;
import static redis.clients.jedis.Protocol.Keyword.RESETSTAT;
import static redis.clients.jedis.Protocol.Keyword.REWRITE;
import static redis.clients.jedis.Protocol.Keyword.SETNAME;

/**
 * Implementation of {@link RedisServerCommands} for {@link JedisClientConnection}.
 * <p>
 * <b>Note:</b> Many server commands in this class use {@code sendCommand} to send raw Redis protocol commands because
 * the corresponding APIs are missing from the {@link UnifiedJedis} interface. These methods exist in the legacy
 * {@code Jedis} class but have not been exposed through {@code UnifiedJedis} as of Jedis 7.2. Once these APIs are added
 * to {@code UnifiedJedis}, the implementations should be updated to use the proper API methods instead of raw commands.
 * <p>
 * Missing APIs include: {@code bgrewriteaof()}, {@code bgsave()}, {@code lastsave()}, {@code save()}, {@code dbSize()},
 * {@code flushDB(FlushMode)}, {@code flushAll(FlushMode)}, {@code shutdown()}, {@code shutdown(SaveMode)},
 * {@code configGet(String)}, {@code configSet(String, String)}, {@code configResetStat()}, {@code configRewrite()},
 * {@code time()}, {@code clientKill(String)}, {@code clientSetname(byte[])}, {@code clientGetname()},
 * {@code clientList()}, {@code replicaof(String, int)}, and {@code replicaofNoOne()}.
 *
 * @author Tihomir Mateev
 * @since 4.1
 */
@NullUnmarked
class JedisClientServerCommands implements RedisServerCommands {

	private final JedisClientConnection connection;

	JedisClientServerCommands(@NonNull JedisClientConnection connection) {
		this.connection = connection;
	}

	@Override
	public void bgReWriteAof() {
		connection.execute(client -> client.sendCommand(BGREWRITEAOF, new byte[0][]),
				pipeline -> pipeline.sendCommand(BGREWRITEAOF, new byte[0][]));
	}

	@Override
	public void bgSave() {
		connection.executeStatus(client -> client.sendCommand(BGSAVE, new byte[0][]),
				pipeline -> pipeline.sendCommand(BGSAVE, new byte[0][]));
	}

	@Override
	public Long lastSave() {
		return connection.execute(client -> client.sendCommand(LASTSAVE, new byte[0][]),
				pipeline -> pipeline.sendCommand(LASTSAVE, new byte[0][]), result -> (Long) result);
	}

	@Override
	public void save() {
		connection.executeStatus(client -> client.sendCommand(SAVE, new byte[0][]),
				pipeline -> pipeline.sendCommand(SAVE, new byte[0][]));
	}

	@Override
	public Long dbSize() {
		return connection.execute(client -> client.sendCommand(DBSIZE, new byte[0][]),
				pipeline -> pipeline.sendCommand(DBSIZE, new byte[0][]), result -> (Long) result);
	}

	@Override
	public void flushDb() {
		connection.executeStatus(UnifiedJedis::flushDB, pipeline -> pipeline.sendCommand(FLUSHDB, new byte[0][]));
	}

	@Override
	public void flushDb(@NonNull FlushOption option) {
		connection.executeStatus(client -> client.sendCommand(FLUSHDB, toBytes(toFlushMode(option).toString())),
				pipeline -> pipeline.sendCommand(FLUSHDB, toBytes(toFlushMode(option).toString())));
	}

	@Override
	public void flushAll() {
		connection.executeStatus(UnifiedJedis::flushAll, pipeline -> pipeline.sendCommand(FLUSHALL, new byte[0][]));
	}

	@Override
	public void flushAll(@NonNull FlushOption option) {
		connection.executeStatus(client -> client.sendCommand(FLUSHALL, toBytes(toFlushMode(option).toString())),
				pipeline -> pipeline.sendCommand(FLUSHALL, toBytes(toFlushMode(option).toString())));
	}

	@Override
	public Properties info() {
		return connection.execute(UnifiedJedis::info, pipeline -> pipeline.sendCommand(INFO, new byte[0][]), result -> {
			String str = result instanceof String ? (String) result : JedisConverters.toString((byte[]) result);
			return toProperties(str);
		});
	}

	@Override
	public Properties info(@NonNull String section) {

		Assert.notNull(section, "Section must not be null");

		return connection.execute(client -> client.info(section), pipeline -> pipeline.sendCommand(INFO, toBytes(section)),
				result -> {
					String str = result instanceof String ? (String) result : JedisConverters.toString((byte[]) result);
					return toProperties(str);
				});
	}

	@Override
	public void shutdown() {
		connection.execute(client -> client.sendCommand(SHUTDOWN, new byte[0][]),
				pipeline -> pipeline.sendCommand(SHUTDOWN, new byte[0][]));
	}

	@Override
	public void shutdown(@Nullable ShutdownOption option) {

		if (option == null) {
			shutdown();
			return;
		}

		SaveMode saveMode = (option == ShutdownOption.NOSAVE) ? SaveMode.NOSAVE : SaveMode.SAVE;
		connection.execute(client -> client.sendCommand(SHUTDOWN, toBytes(saveMode.toString())),
				pipeline -> pipeline.sendCommand(SHUTDOWN, toBytes(saveMode.toString())));
	}

	@Override
	public Properties getConfig(@NonNull String pattern) {

		Assert.notNull(pattern, "Pattern must not be null");

		return connection.execute(client -> client.sendCommand(CONFIG, toBytes(GET.toString()), toBytes(pattern)),
				pipeline -> pipeline.sendCommand(CONFIG, toBytes(GET.toString()), toBytes(pattern)), result -> {
					@SuppressWarnings("unchecked")
					List<byte[]> byteList = (List<byte[]>) result;
					List<String> stringResult = byteList.stream().map(JedisConverters::toString).toList();
					return toProperties(stringResult);
				});
	}

	@Override
	public void setConfig(@NonNull String param, @NonNull String value) {

		Assert.notNull(param, "Parameter must not be null");
		Assert.notNull(value, "Value must not be null");

		connection.execute(client -> client.sendCommand(CONFIG, toBytes(SET.toString()), toBytes(param), toBytes(value)),
				pipeline -> pipeline.sendCommand(CONFIG, toBytes(SET.toString()), toBytes(param), toBytes(value)));
	}

	@Override
	public void resetConfigStats() {
		connection.execute(client -> client.sendCommand(CONFIG, toBytes(RESETSTAT.toString())),
				pipeline -> pipeline.sendCommand(CONFIG, toBytes(RESETSTAT.toString())));
	}

	@Override
	public void rewriteConfig() {
		connection.execute(client -> client.sendCommand(CONFIG, toBytes(REWRITE.toString())),
				pipeline -> pipeline.sendCommand(CONFIG, toBytes(REWRITE.toString())));
	}

	@Override
	public Long time(@NonNull TimeUnit timeUnit) {

		Assert.notNull(timeUnit, "TimeUnit must not be null");

		return connection.execute(client -> client.sendCommand(TIME, new byte[0][]),
				pipeline -> pipeline.sendCommand(TIME, new byte[0][]), result -> {
					@SuppressWarnings("unchecked")
					List<byte[]> byteList = (List<byte[]>) result;
					List<String> stringResult = byteList.stream().map(JedisConverters::toString).toList();
					return toTime(stringResult, timeUnit);
				});
	}

	@Override
	public void killClient(@NonNull String host, int port) {

		Assert.hasText(host, "Host for 'CLIENT KILL' must not be 'null' or 'empty'");

		connection.execute(
				client -> client.sendCommand(CLIENT, toBytes(KILL.toString()), toBytes("%s:%s".formatted(host, port))),
				pipeline -> pipeline.sendCommand(CLIENT, toBytes(KILL.toString()), toBytes("%s:%s".formatted(host, port))));
	}

	@Override
	public void setClientName(byte @NonNull [] name) {

		Assert.notNull(name, "Name must not be null");

		connection.execute(client -> client.sendCommand(CLIENT, toBytes(SETNAME.toString()), name),
				pipeline -> pipeline.sendCommand(CLIENT, toBytes(SETNAME.toString()), name));
	}

	@Override
	public String getClientName() {
		return connection.execute(client -> client.sendCommand(CLIENT, toBytes(GETNAME.toString())),
				pipeline -> pipeline.sendCommand(CLIENT, toBytes(GETNAME.toString())),
				result -> JedisConverters.toString((byte[]) result));
	}

	@Override
	public List<@NonNull RedisClientInfo> getClientList() {
		return connection.execute(client -> client.sendCommand(CLIENT, toBytes(LIST.toString())),
				pipeline -> pipeline.sendCommand(CLIENT, toBytes(LIST.toString())), result -> {
					String str = JedisConverters.toString((byte[]) result);
					return toListOfRedisClientInformation(str);
				});
	}

	@Override
	public void replicaOf(@NonNull String host, int port) {

		Assert.hasText(host, "Host must not be null for 'REPLICAOF' command");

		connection.execute(client -> client.sendCommand(REPLICAOF, toBytes(host), toBytes(String.valueOf(port))),
				pipeline -> pipeline.sendCommand(REPLICAOF, toBytes(host), toBytes(String.valueOf(port))));
	}

	@Override
	public void replicaOfNoOne() {
		connection.execute(client -> client.sendCommand(REPLICAOF, toBytes(NO.toString()), toBytes(ONE.toString())),
				pipeline -> pipeline.sendCommand(REPLICAOF, toBytes(NO.toString()), toBytes(ONE.toString())));
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
		if (option != null) {
			if (option == MigrateOption.COPY) {
				params.copy();
			} else if (option == MigrateOption.REPLACE) {
				params.replace();
			}
		}

		connection.execute(
				client -> client.migrate(target.getRequiredHost(), target.getRequiredPort(), timeoutToUse, params, key),
				pipeline -> pipeline.migrate(target.getRequiredHost(), target.getRequiredPort(), timeoutToUse, params, key));
	}

}
