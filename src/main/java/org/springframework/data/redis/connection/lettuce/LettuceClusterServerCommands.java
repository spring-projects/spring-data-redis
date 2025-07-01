/*
 * Copyright 2017-2025 the original author or authors.
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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.ClusterCommandExecutor.MultiNodeResult;
import org.springframework.data.redis.connection.ClusterCommandExecutor.NodeResult;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisClusterServerCommands;
import org.springframework.data.redis.connection.convert.Converters;
import org.springframework.data.redis.connection.lettuce.LettuceClusterConnection.LettuceClusterCommandCallback;
import org.springframework.data.redis.core.types.RedisClientInfo;
import org.springframework.util.Assert;

/**
 * @author Mark Paluch
 * @author Dennis Neufeld
 * @since 2.0
 */
@NullUnmarked
class LettuceClusterServerCommands extends LettuceServerCommands implements RedisClusterServerCommands {

	private final LettuceClusterConnection connection;

	LettuceClusterServerCommands(@NonNull LettuceClusterConnection connection) {

		super(connection);
		this.connection = connection;
	}

	@Override
	public void bgReWriteAof(@NonNull RedisClusterNode node) {
		executeCommandOnSingleNode(RedisServerCommands::bgrewriteaof, node);
	}

	@Override
	public void bgSave(@NonNull RedisClusterNode node) {
		executeCommandOnSingleNode(RedisServerCommands::bgsave, node);
	}

	@Override
	public Long lastSave(@NonNull RedisClusterNode node) {
		return executeCommandOnSingleNode(client -> client.lastsave().getTime(), node).getValue();
	}

	@Override
	public void save(@NonNull RedisClusterNode node) {
		executeCommandOnSingleNode(RedisServerCommands::save, node);
	}

	@Override
	public Long dbSize(@NonNull RedisClusterNode node) {
		return executeCommandOnSingleNode(RedisServerCommands::dbsize, node).getValue();
	}

	@Override
	public void flushDb(@NonNull RedisClusterNode node) {
		executeCommandOnSingleNode(RedisServerCommands::flushdb, node);
	}

	@Override
	public void flushDb(@NonNull RedisClusterNode node, @NonNull FlushOption option) {
		executeCommandOnSingleNode(it -> it.flushdb(LettuceConverters.toFlushMode(option)), node);
	}

	@Override
	public void flushAll(@NonNull RedisClusterNode node) {
		executeCommandOnSingleNode(RedisServerCommands::flushall, node);
	}

	@Override
	public void flushAll(@NonNull RedisClusterNode node, @NonNull FlushOption option) {
		executeCommandOnSingleNode(it -> it.flushall(LettuceConverters.toFlushMode(option)), node);
	}

	@Override
	public Properties info(@NonNull RedisClusterNode node) {
		return LettuceConverters.toProperties(executeCommandOnSingleNode(RedisServerCommands::info, node).getValue());
	}

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

	@Override
	public Properties info(@NonNull String section) {

		Assert.hasText(section, "Section must not be null or empty");

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

	@Override
	public Properties info(@NonNull RedisClusterNode node, @NonNull String section) {

		Assert.hasText(section, "Section must not be null or empty");

		return LettuceConverters.toProperties(executeCommandOnSingleNode(client -> client.info(section), node).getValue());
	}

	@Override
	public void shutdown(@NonNull RedisClusterNode node) {

		executeCommandOnSingleNode(client -> {
			client.shutdown(true);
			return Void.class;
		}, node);
	}

	@Override
	public Properties getConfig(@NonNull String pattern) {

		Assert.hasText(pattern, "Pattern must not be null or empty");

		List<NodeResult<Map<String, String>>> mapResult = executeCommandOnAllNodes(client -> client.configGet(pattern))
				.getResults();

		Properties properties = new Properties();

		for (NodeResult<Map<String, String>> entry : mapResult) {

			String prefix = entry.getNode().asString();
			entry.getValue().forEach((key, value) -> properties.setProperty(prefix + "." + key, value));
		}

		return properties;
	}

	@Override
	public Properties getConfig(@NonNull RedisClusterNode node, @NonNull String pattern) {

		Assert.hasText(pattern, "Pattern must not be null or empty");

		return executeCommandOnSingleNode(client -> Converters.toProperties(client.configGet(pattern)), node).getValue();
	}

	@Override
	public void setConfig(@NonNull String param, @NonNull String value) {

		Assert.hasText(param, "Parameter must not be null or empty");
		Assert.notNull(value, "Value must not be null");

		executeCommandOnAllNodes(client -> client.configSet(param, value));
	}

	@Override
	public void setConfig(@NonNull RedisClusterNode node, @NonNull String param, @NonNull String value) {

		Assert.hasText(param, "Parameter must not be null or empty");
		Assert.hasText(value, "Value must not be null or empty");

		executeCommandOnSingleNode(client -> client.configSet(param, value), node);
	}

	@Override
	public void resetConfigStats() {
		executeCommandOnAllNodes(RedisServerCommands::configResetstat);
	}

	@Override
	public void resetConfigStats(@NonNull RedisClusterNode node) {
		executeCommandOnSingleNode(RedisServerCommands::configResetstat, node);
	}

	@Override
	public void rewriteConfig() {
		executeCommandOnAllNodes(RedisServerCommands::configRewrite);
	}

	@Override
	public void rewriteConfig(@NonNull RedisClusterNode node) {
		executeCommandOnSingleNode(RedisServerCommands::configRewrite, node);
	}

	@Override
	public Long time(@NonNull RedisClusterNode node, @NonNull TimeUnit timeUnit) {
		return convertListOfStringToTime(executeCommandOnSingleNode(RedisServerCommands::time, node).getValue(), timeUnit);
	}

	@Override
	public List<RedisClientInfo> getClientList() {

		List<String> map = executeCommandOnAllNodes(RedisServerCommands::clientList).resultsAsList();

		ArrayList<RedisClientInfo> result = new ArrayList<>();
		for (String infos : map) {
			result.addAll(LettuceConverters.toListOfRedisClientInformation(infos));
		}
		return result;
	}

	@Override
	public List<RedisClientInfo> getClientList(@NonNull RedisClusterNode node) {

		return LettuceConverters
				.toListOfRedisClientInformation(executeCommandOnSingleNode(RedisServerCommands::clientList, node).getValue());
	}

	@Override
	public void replicaOf(@NonNull String host, int port) {
		throw new InvalidDataAccessApiUsageException(
				"REPLICAOF is not supported in cluster environment; Please use CLUSTER REPLICATE.");
	}

	@Override
	public void replicaOfNoOne() {
		throw new InvalidDataAccessApiUsageException(
				"REPLICAOF is not supported in cluster environment; Please use CLUSTER REPLICATE.");
	}

	private <T> NodeResult<T> executeCommandOnSingleNode(LettuceClusterCommandCallback<T> command,
			RedisClusterNode node) {
		return connection.getClusterCommandExecutor().executeCommandOnSingleNode(command, node);
	}

	private <T> MultiNodeResult<T> executeCommandOnAllNodes(final LettuceClusterCommandCallback<T> cmd) {
		return connection.getClusterCommandExecutor().executeCommandOnAllNodes(cmd);
	}

	private static Long convertListOfStringToTime(List<byte[]> serverTimeInformation, TimeUnit timeUnit) {

		Assert.notEmpty(serverTimeInformation, "Received invalid result from server; Expected 2 items in collection.");
		Assert.isTrue(serverTimeInformation.size() == 2,
				"Received invalid number of arguments from redis server; Expected 2 received " + serverTimeInformation.size());

		return Converters.toTimeMillis(LettuceConverters.toString(serverTimeInformation.get(0)),
				LettuceConverters.toString(serverTimeInformation.get(1)), timeUnit);
	}
}
