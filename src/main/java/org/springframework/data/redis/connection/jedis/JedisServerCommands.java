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
package org.springframework.data.redis.connection.jedis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.args.SaveMode;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.RedisServerCommands;
import org.springframework.data.redis.connection.convert.Converters;
import org.springframework.data.redis.core.types.RedisClientInfo;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * @author Mark Paluch
 * @author Dennis Neufeld
 * @since 2.0
 */
class JedisServerCommands implements RedisServerCommands {

	private final JedisConnection connection;

	JedisServerCommands(JedisConnection connection) {
		this.connection = connection;
	}

	@Override
	public void bgReWriteAof() {
		connection.invoke().just(Jedis::bgrewriteaof);
	}

	@Override
	public void bgSave() {
		connection.invokeStatus().just(Jedis::bgsave);
	}

	@Override
	public Long lastSave() {
		return connection.invoke().just(Jedis::lastsave);
	}

	@Override
	public void save() {
		connection.invokeStatus().just(Jedis::save);
	}

	@Override
	public Long dbSize() {
		return connection.invoke().just(Jedis::dbSize);
	}

	@Override
	public void flushDb() {
		connection.invokeStatus().just(Jedis::flushDB);
	}

	@Override
	public void flushDb(FlushOption option) {
		connection.invokeStatus().just(j -> j.flushDB(JedisConverters.toFlushMode(option)));
	}

	@Override
	public void flushAll() {
		connection.invokeStatus().just(Jedis::flushAll);
	}

	@Override
	public void flushAll(FlushOption option) {
		connection.invokeStatus().just(j -> j.flushAll(JedisConverters.toFlushMode(option)));
	}

	@Override
	public Properties info() {
		return connection.invoke().from(Jedis::info).get(JedisConverters::toProperties);
	}

	@Override
	public Properties info(String section) {

		Assert.notNull(section, "Section must not be null!");

		return connection.invoke().from(j -> j.info(section)).get(JedisConverters::toProperties);
	}

	@Override
	public void shutdown() {
		connection.invokeStatus().just(jedis -> {
			jedis.shutdown();
			return null;
		});
	}

	@Override
	public void shutdown(ShutdownOption option) {

		if (option == null) {
			shutdown();
			return;
		}

		SaveMode saveMode = (option == ShutdownOption.NOSAVE) ? SaveMode.NOSAVE : SaveMode.SAVE;

		connection.getJedis().shutdown(saveMode);
	}

	@Override
	public Properties getConfig(String pattern) {

		Assert.notNull(pattern, "Pattern must not be null!");

		return connection.invoke().from(j -> j.configGet(pattern)).get(Converters::toProperties);
	}

	@Override
	public void setConfig(String param, String value) {

		Assert.notNull(param, "Parameter must not be null!");
		Assert.notNull(value, "Value must not be null!");

		connection.invokeStatus().just(j -> j.configSet(param, value));
	}

	@Override
	public void resetConfigStats() {
		connection.invokeStatus().just(Jedis::configResetStat);
	}

	@Override
	public void rewriteConfig() {
		connection.invokeStatus().just(Jedis::configRewrite);
	}

	@Override
	public Long time(TimeUnit timeUnit) {

		Assert.notNull(timeUnit, "TimeUnit must not be null.");

		return connection.invoke().from(Jedis::time).get((List<String> source) -> JedisConverters.toTime(source, timeUnit));
	}

	@Override
	public void killClient(String host, int port) {

		Assert.hasText(host, "Host for 'CLIENT KILL' must not be 'null' or 'empty'.");

		connection.invokeStatus().just(it -> it.clientKill(String.format("%s:%s", host, port)));
	}

	@Override
	public void setClientName(byte[] name) {

		Assert.notNull(name, "Name must not be null!");

		connection.invokeStatus().just(it -> it.clientSetname(name));
	}

	@Override
	public String getClientName() {
		return connection.invokeStatus().just(Jedis::clientGetname);
	}

	@Override
	public List<RedisClientInfo> getClientList() {
		return connection.invokeStatus().from(Jedis::clientList).get(JedisConverters::toListOfRedisClientInformation);
	}

	@Override
	public void replicaOf(String host, int port) {

		Assert.hasText(host, "Host must not be null for 'REPLICAOF' command.");

		connection.invokeStatus().just(it -> it.replicaof(host, port));
	}

	@Override
	public void replicaOfNoOne() {
		connection.invokeStatus().just(Jedis::replicaofNoOne);
	}

	@Override
	public void migrate(byte[] key, RedisNode target, int dbIndex, @Nullable MigrateOption option) {
		migrate(key, target, dbIndex, option, Long.MAX_VALUE);
	}

	@Override
	public void migrate(byte[] key, RedisNode target, int dbIndex, @Nullable MigrateOption option, long timeout) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(target, "Target node must not be null!");

		int timeoutToUse = timeout <= Integer.MAX_VALUE ? (int) timeout : Integer.MAX_VALUE;

		connection.invokeStatus().just(j -> j.migrate(target.getHost(), target.getPort(), key, dbIndex, timeoutToUse));
	}

}
