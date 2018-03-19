/*
 * Copyright 2015-2018 the original author or authors.
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

import static org.springframework.util.Assert.*;
import static org.springframework.util.StringUtils.*;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.PropertySource;
import org.springframework.data.redis.connection.RedisConfiguration.ClusterConfiguration;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.NumberUtils;
import org.springframework.util.StringUtils;

/**
 * Configuration class used for setting up {@link RedisConnection} via {@link RedisConnectionFactory} using connecting
 * to <a href="http://redis.io/topics/cluster-spec">Redis Cluster</a>. Useful when setting up a high availability Redis
 * environment.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.7
 */
public class RedisClusterConfiguration implements RedisConfiguration, ClusterConfiguration {

	private static final String REDIS_CLUSTER_NODES_CONFIG_PROPERTY = "spring.redis.cluster.nodes";
	private static final String REDIS_CLUSTER_MAX_REDIRECTS_CONFIG_PROPERTY = "spring.redis.cluster.max-redirects";

	private Set<RedisNode> clusterNodes;
	private @Nullable Integer maxRedirects;
	private RedisPassword password = RedisPassword.none();

	/**
	 * Creates new {@link RedisClusterConfiguration}.
	 */
	public RedisClusterConfiguration() {
		this(new MapPropertySource("RedisClusterConfiguration", Collections.emptyMap()));
	}

	/**
	 * Creates {@link RedisClusterConfiguration} for given hostPort combinations.
	 *
	 * <pre>
	 * <code>
	 * clusterHostAndPorts[0] = 127.0.0.1:23679
	 * clusterHostAndPorts[1] = 127.0.0.1:23680 ...
	 * </code>
	 *
	 * <pre>
	 *
	 * @param clusterNodes must not be {@literal null}.
	 */
	public RedisClusterConfiguration(Collection<String> clusterNodes) {
		this(new MapPropertySource("RedisClusterConfiguration", asMap(clusterNodes, -1)));
	}

	/**
	 * Creates {@link RedisClusterConfiguration} looking up values in given {@link PropertySource}.
	 *
	 * <pre>
	 * <code>
	 * spring.redis.cluster.nodes=127.0.0.1:23679,127.0.0.1:23680,127.0.0.1:23681
	 * spring.redis.cluster.timeout=5
	 * spring.redis.cluster.max-redirects=3
	 * spring.redis.cluster.password=foobar
	 * </code>
	 * </pre>
	 *
	 * @param propertySource must not be {@literal null}.
	 */
	public RedisClusterConfiguration(PropertySource<?> propertySource) {

		notNull(propertySource, "PropertySource must not be null!");

		this.clusterNodes = new LinkedHashSet<>();

		if (propertySource.containsProperty(REDIS_CLUSTER_NODES_CONFIG_PROPERTY)) {
			appendClusterNodes(
					commaDelimitedListToSet(propertySource.getProperty(REDIS_CLUSTER_NODES_CONFIG_PROPERTY).toString()));
		}
		if (propertySource.containsProperty(REDIS_CLUSTER_MAX_REDIRECTS_CONFIG_PROPERTY)) {
			this.maxRedirects = NumberUtils.parseNumber(
					propertySource.getProperty(REDIS_CLUSTER_MAX_REDIRECTS_CONFIG_PROPERTY).toString(), Integer.class);
		}
	}

	/**
	 * Set {@literal cluster nodes} to connect to.
	 *
	 * @param nodes must not be {@literal null}.
	 */
	public void setClusterNodes(Iterable<RedisNode> nodes) {

		notNull(nodes, "Cannot set cluster nodes to 'null'.");

		this.clusterNodes.clear();

		for (RedisNode clusterNode : nodes) {
			addClusterNode(clusterNode);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConfiguration.ClusterConfiguration#getClusterNodes()
	 */
	@Override
	public Set<RedisNode> getClusterNodes() {
		return Collections.unmodifiableSet(clusterNodes);
	}

	/**
	 * Add a cluster node to configuration.
	 *
	 * @param node must not be {@literal null}.
	 */
	public void addClusterNode(RedisNode node) {

		notNull(node, "ClusterNode must not be 'null'.");
		this.clusterNodes.add(node);
	}

	/**
	 * @return this.
	 */
	public RedisClusterConfiguration clusterNode(RedisNode node) {

		this.clusterNodes.add(node);
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConfiguration.ClusterConfiguration#getMaxRedirects()
	 */
	@Override
	public Integer getMaxRedirects() {
		return maxRedirects != null && maxRedirects > Integer.MIN_VALUE ? maxRedirects : null;
	}

	/**
	 * @param maxRedirects the max number of redirects to follow.
	 */
	public void setMaxRedirects(int maxRedirects) {

		Assert.isTrue(maxRedirects >= 0, "MaxRedirects must be greater or equal to 0");
		this.maxRedirects = maxRedirects;
	}

	/**
	 * @param host Redis cluster node host name or ip address.
	 * @param port Redis cluster node port.
	 * @return this.
	 */
	public RedisClusterConfiguration clusterNode(String host, Integer port) {
		return clusterNode(new RedisNode(host, port));
	}

	private void appendClusterNodes(Set<String> hostAndPorts) {

		for (String hostAndPort : hostAndPorts) {
			addClusterNode(readHostAndPortFromString(hostAndPort));
		}
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
	}

	private RedisNode readHostAndPortFromString(String hostAndPort) {

		String[] args = split(hostAndPort, ":");

		notNull(args, "HostAndPort need to be seperated by  ':'.");
		isTrue(args.length == 2, "Host and Port String needs to specified as host:port");
		return new RedisNode(args[0], Integer.valueOf(args[1]));
	}

	/**
	 * @param clusterHostAndPorts must not be {@literal null} or empty.
	 * @param redirects the max number of redirects to follow.
	 * @return cluster config map with properties.
	 */
	private static Map<String, Object> asMap(Collection<String> clusterHostAndPorts, int redirects) {

		notNull(clusterHostAndPorts, "ClusterHostAndPorts must not be null!");

		Map<String, Object> map = new HashMap<>();
		map.put(REDIS_CLUSTER_NODES_CONFIG_PROPERTY, StringUtils.collectionToCommaDelimitedString(clusterHostAndPorts));

		if (redirects >= 0) {
			map.put(REDIS_CLUSTER_MAX_REDIRECTS_CONFIG_PROPERTY, redirects);
		}

		return map;
	}
}
