/*
 * Copyright 2017-2021 the original author or authors.
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

import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.MultiKeyPipelineBase;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.RedisServerCommands;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.connection.convert.Converters;
import org.springframework.data.redis.core.types.RedisClientInfo;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * @author Mark Paluch
 * @since 2.0
 */
class JedisServerCommands implements RedisServerCommands {

	private static final String SHUTDOWN_SCRIPT = "return redis.call('SHUTDOWN','%s')";

	private final JedisConnection connection;

	JedisServerCommands(JedisConnection connection) {
		this.connection = connection;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#bgReWriteAof()
	 */
	@Override
	public void bgReWriteAof() {
		connection.invoke().just(BinaryJedis::bgrewriteaof, MultiKeyPipelineBase::bgrewriteaof);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#bgSave()
	 */
	@Override
	public void bgSave() {
		connection.invokeStatus().just(BinaryJedis::bgsave, MultiKeyPipelineBase::bgsave);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#lastSave()
	 */
	@Override
	public Long lastSave() {
		return connection.invoke().just(BinaryJedis::lastsave, MultiKeyPipelineBase::lastsave);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#save()
	 */
	@Override
	public void save() {
		connection.invokeStatus().just(BinaryJedis::save, MultiKeyPipelineBase::save);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#dbSize()
	 */
	@Override
	public Long dbSize() {
		return connection.invoke().just(BinaryJedis::dbSize, MultiKeyPipelineBase::dbSize);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#flushDb()
	 */
	@Override
	public void flushDb() {
		connection.invokeStatus().just(BinaryJedis::flushDB, MultiKeyPipelineBase::flushDB);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#flushAll()
	 */
	@Override
	public void flushAll() {
		connection.invokeStatus().just(BinaryJedis::flushAll, MultiKeyPipelineBase::flushAll);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#info()
	 */
	@Override
	public Properties info() {
		return connection.invoke().from(BinaryJedis::info, MultiKeyPipelineBase::info).get(JedisConverters::toProperties);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#info(java.lang.String)
	 */
	@Override
	public Properties info(String section) {

		Assert.notNull(section, "Section must not be null!");

		return connection.invoke().from(BinaryJedis::info, MultiKeyPipelineBase::info, section)
				.get(JedisConverters::toProperties);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#shutdown()
	 */
	@Override
	public void shutdown() {
		connection.invokeStatus().just(BinaryJedis::shutdown, MultiKeyPipelineBase::shutdown);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#shutdown(org.springframework.data.redis.connection.RedisServerCommands.ShutdownOption)
	 */
	@Override
	public void shutdown(ShutdownOption option) {

		if (option == null) {
			shutdown();
			return;
		}

		connection.eval(String.format(SHUTDOWN_SCRIPT, option.name()).getBytes(), ReturnType.STATUS, 0);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#getConfig(java.lang.String)
	 */
	@Override
	public Properties getConfig(String pattern) {

		Assert.notNull(pattern, "Pattern must not be null!");

		return connection.invoke().from(Jedis::configGet, MultiKeyPipelineBase::configGet, pattern)
				.get(Converters::toProperties);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#setConfig(java.lang.String, java.lang.String)
	 */
	@Override
	public void setConfig(String param, String value) {

		Assert.notNull(param, "Parameter must not be null!");
		Assert.notNull(value, "Value must not be null!");

		connection.invokeStatus().just(Jedis::configSet, MultiKeyPipelineBase::configSet, param, value);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#resetConfigStats()
	 */
	@Override
	public void resetConfigStats() {
		connection.invokeStatus().just(BinaryJedis::configResetStat, MultiKeyPipelineBase::configResetStat);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#time(TimeUnit)
	 */
	@Override
	public Long time(TimeUnit timeUnit) {

		Assert.notNull(timeUnit, "TimeUnit must not be null.");

		return connection.invoke().from(BinaryJedis::time, MultiKeyPipelineBase::time)
				.get((List<String> source) -> JedisConverters.toTime(source, timeUnit));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#killClient(java.lang.String, int)
	 */
	@Override
	public void killClient(String host, int port) {

		Assert.hasText(host, "Host for 'CLIENT KILL' must not be 'null' or 'empty'.");

		if (isQueueing() || isPipelined()) {
			throw new UnsupportedOperationException("'CLIENT KILL' is not supported in transaction / pipline mode.");
		}

		connection.invokeStatus().just(it -> it.clientKill(String.format("%s:%s", host, port)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#setClientName(byte[])
	 */
	@Override
	public void setClientName(byte[] name) {

		Assert.notNull(name, "Name must not be null!");

		if (isPipelined() || isQueueing()) {
			throw new UnsupportedOperationException("'CLIENT SETNAME' is not suppored in transacton / pipeline mode.");
		}

		connection.invokeStatus().just(it -> it.clientSetname(name));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#getClientName()
	 */
	@Override
	public String getClientName() {

		if (isPipelined() || isQueueing()) {
			throw new UnsupportedOperationException();
		}

		return connection.invokeStatus().just(Jedis::clientGetname);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#getClientList()
	 */
	@Override
	public List<RedisClientInfo> getClientList() {

		if (isQueueing() || isPipelined()) {
			throw new UnsupportedOperationException("'CLIENT LIST' is not supported in in pipeline / multi mode.");
		}

		return connection.invokeStatus().from(Jedis::clientList).get(JedisConverters::toListOfRedisClientInformation);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#slaveOf(java.lang.String, int)
	 */
	@Override
	public void slaveOf(String host, int port) {

		Assert.hasText(host, "Host must not be null for 'SLAVEOF' command.");

		if (isQueueing() || isPipelined()) {
			throw new UnsupportedOperationException("'SLAVEOF' cannot be called in pipline / transaction mode.");
		}

		connection.invokeStatus().just(it -> it.slaveof(host, port));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#slaveOfNoOne()
	 */
	@Override
	public void slaveOfNoOne() {

		if (isQueueing() || isPipelined()) {
			throw new UnsupportedOperationException("'SLAVEOF' cannot be called in pipline / transaction mode.");
		}

		connection.invokeStatus().just(BinaryJedis::slaveofNoOne);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#migrate(byte[], org.springframework.data.redis.connection.RedisNode, int, org.springframework.data.redis.connection.RedisServerCommands.MigrateOption)
	 */
	@Override
	public void migrate(byte[] key, RedisNode target, int dbIndex, @Nullable MigrateOption option) {
		migrate(key, target, dbIndex, option, Long.MAX_VALUE);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#migrate(byte[], org.springframework.data.redis.connection.RedisNode, int, org.springframework.data.redis.connection.RedisServerCommands.MigrateOption, long)
	 */
	@Override
	public void migrate(byte[] key, RedisNode target, int dbIndex, @Nullable MigrateOption option, long timeout) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(target, "Target node must not be null!");

		int timeoutToUse = timeout <= Integer.MAX_VALUE ? (int) timeout : Integer.MAX_VALUE;

		connection.invokeStatus().just(BinaryJedis::migrate, MultiKeyPipelineBase::migrate, target.getHost(),
				target.getPort(), key, dbIndex, timeoutToUse);
	}

	private boolean isPipelined() {
		return connection.isPipelined();
	}

	private boolean isQueueing() {
		return connection.isQueueing();
	}

}
