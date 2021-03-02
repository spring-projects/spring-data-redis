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
package org.springframework.data.redis.connection.lettuce;

import io.lettuce.core.api.sync.RedisServerCommands;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.ClusterCommandExecutor.MultiNodeResult;
import org.springframework.data.redis.connection.ClusterCommandExecutor.NodeResult;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisClusterServerCommands;
import org.springframework.data.redis.connection.convert.Converters;
import org.springframework.data.redis.connection.lettuce.LettuceClusterConnection.LettuceClusterCommandCallback;
import org.springframework.data.redis.core.types.RedisClientInfo;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

/**
 * @author Mark Paluch
 * @since 2.0
 */
class LettuceClusterServerCommands extends LettuceServerCommands implements RedisClusterServerCommands {

	private final LettuceClusterConnection connection;

	LettuceClusterServerCommands(LettuceClusterConnection connection) {

		super(connection);
		this.connection = connection;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterServerCommands#bgReWriteAof(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void bgReWriteAof(RedisClusterNode node) {
		executeCommandOnSingleNode(RedisServerCommands::bgrewriteaof, node);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterServerCommands#bgSave(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void bgSave(RedisClusterNode node) {
		executeCommandOnSingleNode(RedisServerCommands::bgsave, node);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterServerCommands#lastSave(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public Long lastSave(RedisClusterNode node) {
		return executeCommandOnSingleNode(client -> client.lastsave().getTime(), node).getValue();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterServerCommands#save(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void save(RedisClusterNode node) {
		executeCommandOnSingleNode(RedisServerCommands::save, node);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceServerCommands#dbSize()
	 */
	@Override
	public Long dbSize() {

		Collection<Long> dbSizes = executeCommandOnAllNodes(RedisServerCommands::dbsize).resultsAsList();

		if (CollectionUtils.isEmpty(dbSizes)) {
			return 0L;
		}

		Long size = 0L;
		for (Long value : dbSizes) {
			size += value;
		}
		return size;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterServerCommands#dbSize(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public Long dbSize(RedisClusterNode node) {
		return executeCommandOnSingleNode(RedisServerCommands::dbsize, node).getValue();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceServerCommands#flushDb()
	 */
	@Override
	public void flushDb() {
		executeCommandOnAllNodes(RedisServerCommands::flushdb);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterServerCommands#flushDb(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void flushDb(RedisClusterNode node) {
		executeCommandOnSingleNode(RedisServerCommands::flushdb, node);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceServerCommands#flushAll()
	 */
	@Override
	public void flushAll() {
		executeCommandOnAllNodes(RedisServerCommands::flushall);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterServerCommands#flushAll(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void flushAll(RedisClusterNode node) {
		executeCommandOnSingleNode(RedisServerCommands::flushall, node);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterServerCommands#info(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public Properties info(RedisClusterNode node) {
		return LettuceConverters.toProperties(executeCommandOnSingleNode(RedisServerCommands::info, node).getValue());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceServerCommands#info()
	 */
	@Override
	public Properties info() {

		Properties infos = new Properties();

		List<NodeResult<Properties>> nodeResults = executeCommandOnAllNodes(
				client -> LettuceConverters.toProperties(client.info())).getResults();

		for (NodeResult<Properties> nodePorperties : nodeResults) {
			for (Entry<Object, Object> entry : nodePorperties.getValue().entrySet()) {
				infos.put(nodePorperties.getNode().asString() + "." + entry.getKey(), entry.getValue());
			}
		}

		return infos;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceServerCommands#info(java.lang.String)
	 */
	@Override
	public Properties info(String section) {

		Assert.hasText(section, "Section must not be null or empty!");

		Properties infos = new Properties();
		List<NodeResult<Properties>> nodeResults = executeCommandOnAllNodes(
				client -> LettuceConverters.toProperties(client.info(section))).getResults();

		for (NodeResult<Properties> nodePorperties : nodeResults) {
			for (Entry<Object, Object> entry : nodePorperties.getValue().entrySet()) {
				infos.put(nodePorperties.getNode().asString() + "." + entry.getKey(), entry.getValue());
			}
		}

		return infos;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterServerCommands#info(org.springframework.data.redis.connection.RedisClusterNode, java.lang.String)
	 */
	@Override
	public Properties info(RedisClusterNode node, String section) {

		Assert.hasText(section, "Section must not be null or empty!");

		return LettuceConverters.toProperties(executeCommandOnSingleNode(client -> client.info(section), node).getValue());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterServerCommands#shutdown(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void shutdown(RedisClusterNode node) {

		executeCommandOnSingleNode((LettuceClusterCommandCallback<Void>) client -> {
			client.shutdown(true);
			return null;
		}, node);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceServerCommands#getConfig(java.lang.String)
	 */
	@Override
	public Properties getConfig(String pattern) {

		Assert.hasText(pattern, "Pattern must not be null or empty!");

		List<NodeResult<Map<String, String>>> mapResult = executeCommandOnAllNodes(client -> client.configGet(pattern))
				.getResults();

		Properties properties = new Properties();

		for (NodeResult<Map<String, String>> entry : mapResult) {

			String prefix = entry.getNode().asString();
			entry.getValue().forEach((key, value) -> properties.setProperty(prefix + "." + key, value));
		}

		return properties;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterServerCommands#getConfig(org.springframework.data.redis.connection.RedisClusterNode, java.lang.String)
	 */
	@Override
	public Properties getConfig(RedisClusterNode node, String pattern) {

		Assert.hasText(pattern, "Pattern must not be null or empty!");

		return executeCommandOnSingleNode(client -> Converters.toProperties(client.configGet(pattern)), node).getValue();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceServerCommands#setConfig(java.lang.String, java.lang.String)
	 */
	@Override
	public void setConfig(String param, String value) {

		Assert.hasText(param, "Parameter must not be null or empty!");
		Assert.hasText(value, "Value must not be null or empty!");

		executeCommandOnAllNodes(client -> client.configSet(param, value));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterServerCommands#setConfig(org.springframework.data.redis.connection.RedisClusterNode, java.lang.String, java.lang.String)
	 */
	@Override
	public void setConfig(RedisClusterNode node, String param, String value) {

		Assert.hasText(param, "Parameter must not be null or empty!");
		Assert.hasText(value, "Value must not be null or empty!");

		executeCommandOnSingleNode(client -> client.configSet(param, value), node);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceServerCommands#resetConfigStats()
	 */
	@Override
	public void resetConfigStats() {
		executeCommandOnAllNodes(RedisServerCommands::configResetstat);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterServerCommands#resetConfigStats(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void resetConfigStats(RedisClusterNode node) {
		executeCommandOnSingleNode(RedisServerCommands::configResetstat, node);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterServerCommands#time(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public Long time(RedisClusterNode node, TimeUnit timeUnit) {
		return convertListOfStringToTime(executeCommandOnSingleNode(RedisServerCommands::time, node).getValue(), timeUnit);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceServerCommands#getClientList()
	 */
	@Override
	public List<RedisClientInfo> getClientList() {

		List<String> map = executeCommandOnAllNodes(RedisServerCommands::clientList).resultsAsList();

		ArrayList<RedisClientInfo> result = new ArrayList<>();
		for (String infos : map) {
			result.addAll(LettuceConverters.toListOfRedisClientInformation(infos));
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterServerCommands#getClientList(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public List<RedisClientInfo> getClientList(RedisClusterNode node) {

		return LettuceConverters
				.toListOfRedisClientInformation(executeCommandOnSingleNode(RedisServerCommands::clientList, node).getValue());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceServerCommands#slaveOf(java.lang.String, int)
	 */
	@Override
	public void slaveOf(String host, int port) {
		throw new InvalidDataAccessApiUsageException(
				"SlaveOf is not supported in cluster environment. Please use CLUSTER REPLICATE.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceServerCommands#slaveOfNoOne()
	 */
	@Override
	public void slaveOfNoOne() {
		throw new InvalidDataAccessApiUsageException(
				"SlaveOf is not supported in cluster environment. Please use CLUSTER REPLICATE.");
	}

	private <T> NodeResult<T> executeCommandOnSingleNode(LettuceClusterCommandCallback<T> command,
			RedisClusterNode node) {
		return connection.getClusterCommandExecutor().executeCommandOnSingleNode(command, node);
	}

	private <T> MultiNodeResult<T> executeCommandOnAllNodes(final LettuceClusterCommandCallback<T> cmd) {
		return connection.getClusterCommandExecutor().executeCommandOnAllNodes(cmd);
	}

	private static Long convertListOfStringToTime(List<byte[]> serverTimeInformation, TimeUnit timeUnit) {

		Assert.notEmpty(serverTimeInformation, "Received invalid result from server. Expected 2 items in collection.");
		Assert.isTrue(serverTimeInformation.size() == 2,
				"Received invalid number of arguments from redis server. Expected 2 received " + serverTimeInformation.size());

		return Converters.toTimeMillis(LettuceConverters.toString(serverTimeInformation.get(0)),
				LettuceConverters.toString(serverTimeInformation.get(1)), timeUnit);
	}
}
