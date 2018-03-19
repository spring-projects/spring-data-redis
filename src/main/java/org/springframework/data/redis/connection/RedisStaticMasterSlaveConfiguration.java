/*
 * Copyright 2018 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.springframework.data.redis.connection.RedisConfiguration.StaticMasterSlaveConfiguration;
import org.springframework.util.Assert;

/**
 * Configuration class used for setting up {@link RedisConnection} via {@link RedisConnectionFactory} using the provided
 * Master / Slave configuration to nodes know to not change address. Eg. when connecting to
 * <a href="https://aws.amazon.com/documentation/elasticache/">AWS ElastiCache with Read Replicas</a> .
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.1
 */
public class RedisStaticMasterSlaveConfiguration implements RedisConfiguration, StaticMasterSlaveConfiguration {

	private static final int DEFAULT_PORT = 6379;

	private List<RedisStandaloneConfiguration> nodes = new ArrayList<>();
	private int database;
	private RedisPassword password = RedisPassword.none();

	/**
	 * Create a new {@link StaticMasterSlaveConfiguration} given {@code hostName}.
	 *
	 * @param hostName must not be {@literal null} or empty.
	 */
	public RedisStaticMasterSlaveConfiguration(String hostName) {
		this(hostName, DEFAULT_PORT);
	}

	/**
	 * Create a new {@link StaticMasterSlaveConfiguration} given {@code hostName} and {@code port}.
	 *
	 * @param hostName must not be {@literal null} or empty.
	 * @param port a valid TCP port (1-65535).
	 */
	public RedisStaticMasterSlaveConfiguration(String hostName, int port) {
		addNode(hostName, port);
	}

	/**
	 * Add a {@link RedisStandaloneConfiguration node} to the list of nodes given {@code hostName}.
	 *
	 * @param hostName must not be {@literal null} or empty.
	 * @param port a valid TCP port (1-65535).
	 */
	public void addNode(String hostName, int port) {
		addNode(new RedisStandaloneConfiguration(hostName, port));
	}

	/**
	 * Add a {@link RedisStandaloneConfiguration node} to the list of nodes.
	 *
	 * @param node must not be {@literal null}.
	 */
	private void addNode(RedisStandaloneConfiguration node) {

		Assert.notNull(node, "RedisStandaloneConfiguration must not be null!");

		node.setPassword(password);
		node.setDatabase(database);
		nodes.add(node);
	}

	/**
	 * Add a {@link RedisStandaloneConfiguration node} to the list of nodes given {@code hostName}.
	 *
	 * @param hostName must not be {@literal null} or empty.
	 * @return {@code this} {@link StaticMasterSlaveConfiguration}.
	 */
	public StaticMasterSlaveConfiguration node(String hostName) {
		return node(hostName, DEFAULT_PORT);
	}

	/**
	 * Add a {@link RedisStandaloneConfiguration node} to the list of nodes given {@code hostName} and {@code port}.
	 *
	 * @param hostName must not be {@literal null} or empty.
	 * @param port a valid TCP port (1-65535).
	 * @return {@code this} {@link StaticMasterSlaveConfiguration}.
	 */
	public RedisStaticMasterSlaveConfiguration node(String hostName, int port) {

		addNode(hostName, port);
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConfiguration.WithDatabaseIndex#getDatabase()
	 */
	@Override
	public int getDatabase() {
		return database;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConfiguration.WithDatabaseIndex#setDatabase(int)
	 */
	@Override
	public void setDatabase(int index) {

		Assert.isTrue(index >= 0, () -> String.format("Invalid DB index '%s' (a positive index required)", index));

		this.database = index;
		this.nodes.forEach(it -> it.setDatabase(database));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConfiguration.WithPassword#getPassword()
	 */
	@Override
	public RedisPassword getPassword() {
		return password;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConfiguration.WithPassword#setPassword(org.springframework.data.redis.connection.RedisPassword)
	 */
	@Override
	public void setPassword(RedisPassword password) {

		Assert.notNull(password, "RedisPassword must not be null!");

		this.password = password;
		this.nodes.forEach(it -> it.setPassword(password));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConfiguration.StaticMasterSlaveConfiguration#getNodes()
	 */
	@Override
	public List<RedisStandaloneConfiguration> getNodes() {
		return Collections.unmodifiableList(nodes);
	}
}
