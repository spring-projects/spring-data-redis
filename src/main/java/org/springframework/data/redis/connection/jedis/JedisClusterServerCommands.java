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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.ClusterCommandExecutor.MultiNodeResult;
import org.springframework.data.redis.connection.ClusterCommandExecutor.NodeResult;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisClusterServerCommands;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.convert.Converters;
import org.springframework.data.redis.connection.jedis.JedisClusterConnection.JedisClusterCommandCallback;
import org.springframework.data.redis.core.types.RedisClientInfo;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

/**
 * @author Mark Paluch
 * @author Dennis Neufeld
 * @since 2.0
 */
class JedisClusterServerCommands implements RedisClusterServerCommands {

	private final JedisClusterConnection connection;

	JedisClusterServerCommands(JedisClusterConnection connection) {
		this.connection = connection;
	}

	@Override
	public void bgReWriteAof(RedisClusterNode node) {
		executeCommandOnSingleNode(Jedis::bgrewriteaof, node);
	}

	@Override
	public void bgReWriteAof() {
		connection.getClusterCommandExecutor()
				.executeCommandOnAllNodes((JedisClusterCommandCallback<String>) Jedis::bgrewriteaof);
	}

	@Override
	public void bgSave() {
		connection.getClusterCommandExecutor()
				.executeCommandOnAllNodes((JedisClusterCommandCallback<String>) Jedis::bgsave);
	}

	@Override
	public void bgSave(RedisClusterNode node) {
		executeCommandOnSingleNode(Jedis::bgsave, node);
	}

	@Override
	public Long lastSave() {

		List<Long> result = new ArrayList<>(executeCommandOnAllNodes(Jedis::lastsave).resultsAsList());

		if (CollectionUtils.isEmpty(result)) {
			return null;
		}

		Collections.sort(result, Collections.reverseOrder());
		return result.get(0);
	}

	@Override
	public Long lastSave(RedisClusterNode node) {
		return executeCommandOnSingleNode(Jedis::lastsave, node).getValue();
	}

	@Override
	public void save() {
		executeCommandOnAllNodes(Jedis::save);
	}

	@Override
	public void save(RedisClusterNode node) {
		executeCommandOnSingleNode(Jedis::save, node);
	}

	@Override
	public Long dbSize() {

		Collection<Long> dbSizes = executeCommandOnAllNodes(Jedis::dbSize).resultsAsList();

		if (CollectionUtils.isEmpty(dbSizes)) {
			return 0L;
		}

		Long size = 0L;
		for (Long value : dbSizes) {
			size += value;
		}
		return size;
	}

	@Override
	public Long dbSize(RedisClusterNode node) {
		return executeCommandOnSingleNode(Jedis::dbSize, node).getValue();
	}

	@Override
	public void flushDb() {
		executeCommandOnAllNodes(Jedis::flushDB);
	}

	@Override
	public void flushDb(FlushOption option) {
		executeCommandOnAllNodes(it -> it.flushDB(JedisConverters.toFlushMode(option)));
	}

	@Override
	public void flushDb(RedisClusterNode node) {
		executeCommandOnSingleNode(Jedis::flushDB, node);
	}

	@Override
	public void flushDb(RedisClusterNode node, FlushOption option) {
		executeCommandOnSingleNode(it -> it.flushDB(JedisConverters.toFlushMode(option)), node);
	}

	@Override
	public void flushAll() {
		connection.getClusterCommandExecutor()
				.executeCommandOnAllNodes((JedisClusterCommandCallback<String>) Jedis::flushAll);
	}

	@Override
	public void flushAll(FlushOption option) {
		connection.getClusterCommandExecutor().executeCommandOnAllNodes(
				(JedisClusterCommandCallback<String>) it -> it.flushAll(JedisConverters.toFlushMode(option)));
	}

	@Override
	public void flushAll(RedisClusterNode node) {
		executeCommandOnSingleNode(Jedis::flushAll, node);
	}

	@Override
	public void flushAll(RedisClusterNode node, FlushOption option) {
		executeCommandOnSingleNode(it -> it.flushAll(JedisConverters.toFlushMode(option)), node);
	}

	@Override
	public Properties info() {

		Properties infos = new Properties();

		List<NodeResult<Properties>> nodeResults = connection.getClusterCommandExecutor()
				.executeCommandOnAllNodes(
						(JedisClusterCommandCallback<Properties>) client -> JedisConverters.toProperties(client.info()))
				.getResults();

		for (NodeResult<Properties> nodeProperties : nodeResults) {
			for (Entry<Object, Object> entry : nodeProperties.getValue().entrySet()) {
				infos.put(nodeProperties.getNode().asString() + "." + entry.getKey(), entry.getValue());
			}
		}

		return infos;
	}

	@Override
	public Properties info(RedisClusterNode node) {
		return JedisConverters.toProperties(executeCommandOnSingleNode(Jedis::info, node).getValue());
	}

	@Override
	public Properties info(String section) {

		Assert.notNull(section, "Section must not be null!");

		Properties infos = new Properties();

		List<NodeResult<Properties>> nodeResults = connection.getClusterCommandExecutor()
				.executeCommandOnAllNodes(
						(JedisClusterCommandCallback<Properties>) client -> JedisConverters.toProperties(client.info(section)))
				.getResults();

		for (NodeResult<Properties> nodeProperties : nodeResults) {
			for (Entry<Object, Object> entry : nodeProperties.getValue().entrySet()) {
				infos.put(nodeProperties.getNode().asString() + "." + entry.getKey(), entry.getValue());
			}
		}

		return infos;
	}

	@Override
	public Properties info(RedisClusterNode node, String section) {

		Assert.notNull(section, "Section must not be null!");

		return JedisConverters.toProperties(executeCommandOnSingleNode(client -> client.info(section), node).getValue());
	}

	@Override
	public void shutdown() {
		connection.getClusterCommandExecutor().executeCommandOnAllNodes((JedisClusterCommandCallback<String>) jedis -> {
			jedis.shutdown();
			return null;
		});
	}

	@Override
	public void shutdown(RedisClusterNode node) {
		executeCommandOnSingleNode(jedis -> {
			jedis.shutdown();
			return null;
		}, node);
	}

	@Override
	public void shutdown(ShutdownOption option) {

		if (option == null) {
			shutdown();
			return;
		}

		throw new IllegalArgumentException("Shutdown with options is not supported for jedis.");
	}

	@Override
	public Properties getConfig(String pattern) {

		Assert.notNull(pattern, "Pattern must not be null!");

		List<NodeResult<List<String>>> mapResult = connection.getClusterCommandExecutor()
				.executeCommandOnAllNodes((JedisClusterCommandCallback<List<String>>) client -> client.configGet(pattern))
				.getResults();

		List<String> result = new ArrayList<>();
		for (NodeResult<List<String>> entry : mapResult) {

			String prefix = entry.getNode().asString();
			int i = 0;
			for (String value : entry.getValue()) {
				result.add((i++ % 2 == 0 ? (prefix + ".") : "") + value);
			}
		}

		return Converters.toProperties(result);
	}

	@Override
	public Properties getConfig(RedisClusterNode node, String pattern) {

		Assert.notNull(pattern, "Pattern must not be null!");

		return connection.getClusterCommandExecutor()
				.executeCommandOnSingleNode(
						(JedisClusterCommandCallback<Properties>) client -> Converters.toProperties(client.configGet(pattern)),
						node)
				.getValue();
	}

	@Override
	public void setConfig(String param, String value) {

		Assert.notNull(param, "Parameter must not be null!");
		Assert.notNull(value, "Value must not be null!");

		connection.getClusterCommandExecutor()
				.executeCommandOnAllNodes((JedisClusterCommandCallback<String>) client -> client.configSet(param, value));
	}

	@Override
	public void setConfig(RedisClusterNode node, String param, String value) {

		Assert.notNull(param, "Parameter must not be null!");
		Assert.notNull(value, "Value must not be null!");

		executeCommandOnSingleNode(client -> client.configSet(param, value), node);
	}

	@Override
	public void resetConfigStats() {
		connection.getClusterCommandExecutor()
				.executeCommandOnAllNodes((JedisClusterCommandCallback<String>) Jedis::configResetStat);
	}

	@Override
	public void rewriteConfig() {
		connection.getClusterCommandExecutor()
				.executeCommandOnAllNodes((JedisClusterCommandCallback<String>) Jedis::configRewrite);
	}

	@Override
	public void resetConfigStats(RedisClusterNode node) {
		executeCommandOnSingleNode(Jedis::configResetStat, node);
	}

	@Override
	public void rewriteConfig(RedisClusterNode node) {
		executeCommandOnSingleNode(Jedis::configRewrite, node);
	}

	@Override
	public Long time(TimeUnit timeUnit) {

		return convertListOfStringToTime(
				connection.getClusterCommandExecutor()
						.executeCommandOnArbitraryNode((JedisClusterCommandCallback<List<String>>) Jedis::time).getValue(),
				timeUnit);
	}

	@Override
	public Long time(RedisClusterNode node, TimeUnit timeUnit) {

		return convertListOfStringToTime(
				connection.getClusterCommandExecutor()
						.executeCommandOnSingleNode((JedisClusterCommandCallback<List<String>>) Jedis::time, node).getValue(),
				timeUnit);
	}

	@Override
	public void killClient(String host, int port) {

		Assert.hasText(host, "Host for 'CLIENT KILL' must not be 'null' or 'empty'.");
		String hostAndPort = String.format("%s:%s", host, port);

		connection.getClusterCommandExecutor()
				.executeCommandOnAllNodes((JedisClusterCommandCallback<String>) client -> client.clientKill(hostAndPort));
	}

	@Override
	public void setClientName(byte[] name) {
		throw new InvalidDataAccessApiUsageException("CLIENT SETNAME is not supported in cluster environment.");
	}

	@Override
	public String getClientName() {
		throw new InvalidDataAccessApiUsageException("CLIENT GETNAME is not supported in cluster environment.");
	}

	@Override
	public List<RedisClientInfo> getClientList() {

		Collection<String> map = connection.getClusterCommandExecutor()
				.executeCommandOnAllNodes((JedisClusterCommandCallback<String>) Jedis::clientList).resultsAsList();

		ArrayList<RedisClientInfo> result = new ArrayList<>();
		for (String infos : map) {
			result.addAll(JedisConverters.toListOfRedisClientInformation(infos));
		}
		return result;
	}

	@Override
	public List<RedisClientInfo> getClientList(RedisClusterNode node) {

		return JedisConverters
				.toListOfRedisClientInformation(executeCommandOnSingleNode(Jedis::clientList, node).getValue());
	}

	@Override
	public void replicaOf(String host, int port) {
		throw new InvalidDataAccessApiUsageException(
				"REPLICAOF is not supported in cluster environment. Please use CLUSTER REPLICATE.");
	}

	@Override
	public void replicaOfNoOne() {
		throw new InvalidDataAccessApiUsageException(
				"REPLICAOF is not supported in cluster environment. Please use CLUSTER REPLICATE.");
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

		RedisClusterNode node = connection.getTopologyProvider().getTopology().lookup(target.getHost(), target.getPort());

		executeCommandOnSingleNode(client -> client.migrate(target.getHost(), target.getPort(), key, dbIndex, timeoutToUse),
				node);
	}

	private Long convertListOfStringToTime(List<String> serverTimeInformation, TimeUnit timeUnit) {

		Assert.notEmpty(serverTimeInformation, "Received invalid result from server. Expected 2 items in collection.");
		Assert.isTrue(serverTimeInformation.size() == 2,
				"Received invalid number of arguments from redis server. Expected 2 received " + serverTimeInformation.size());

		return Converters.toTimeMillis(serverTimeInformation.get(0), serverTimeInformation.get(1), timeUnit);
	}

	private <T> NodeResult<T> executeCommandOnSingleNode(JedisClusterCommandCallback<T> cmd, RedisClusterNode node) {
		return connection.getClusterCommandExecutor().executeCommandOnSingleNode(cmd, node);
	}

	private <T> MultiNodeResult<T> executeCommandOnAllNodes(JedisClusterCommandCallback<T> cmd) {
		return connection.getClusterCommandExecutor().executeCommandOnAllNodes(cmd);
	}

}
