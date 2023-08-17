/*
 * Copyright 2015-2023 the original author or authors.
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
package org.springframework.data.redis.connection;

import static org.springframework.util.StringUtils.commaDelimitedListToSet;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.PropertySource;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.data.redis.connection.RedisConfiguration.ClusterConfiguration;
import org.springframework.data.redis.util.RedisAssertions;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.NumberUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * Configuration class used to set up a {@link RedisConnection} via {@link RedisConnectionFactory} for connecting
 * to <a href="https://redis.io/topics/cluster-spec">Redis Cluster</a>. Useful when setting up a highly available Redis
 * environment.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author John Blum
 * @since 1.7
 */
public class RedisClusterConfiguration implements RedisConfiguration, ClusterConfiguration {

	private static final String REDIS_CLUSTER_NODES_CONFIG_PROPERTY = "spring.redis.cluster.nodes";
	private static final String REDIS_CLUSTER_MAX_REDIRECTS_CONFIG_PROPERTY = "spring.redis.cluster.max-redirects";

	private @Nullable Integer maxRedirects;

	private @Nullable AsyncTaskExecutor executor;

	private RedisPassword password = RedisPassword.none();

	private final Set<RedisNode> clusterNodes;

	private @Nullable String username = null;

	/**
	 * Creates a new, default {@link RedisClusterConfiguration}.
	 */
	public RedisClusterConfiguration() {
		this(new MapPropertySource("RedisClusterConfiguration", Collections.emptyMap()));
	}

	/**
	 * Creates a new {@link RedisClusterConfiguration} for given {@link String hostPort} combinations.
	 *
	 * <pre>
	 * <code>
	 * clusterHostAndPorts[0] = 127.0.0.1:23679
	 * clusterHostAndPorts[1] = 127.0.0.1:23680 ...
	 * </code>
	 * </pre>
	 *
	 * @param clusterNodes must not be {@literal null}.
	 */
	public RedisClusterConfiguration(Collection<String> clusterNodes) {
		this(new MapPropertySource("RedisClusterConfiguration", asMap(clusterNodes, -1)));
	}

	/**
	 * Creates a new {@link RedisClusterConfiguration} looking up configuration values from the given
	 * {@link PropertySource}.
	 *
	 * <pre>
	 * <code>
	 * spring.redis.cluster.nodes=127.0.0.1:23679,127.0.0.1:23680,127.0.0.1:23681
	 * spring.redis.cluster.max-redirects=3
	 * </code>
	 * </pre>
	 *
	 * @param propertySource must not be {@literal null}.
	 */
	public RedisClusterConfiguration(PropertySource<?> propertySource) {

		Assert.notNull(propertySource, "PropertySource must not be null");

		this.clusterNodes = new LinkedHashSet<>();

		if (propertySource.containsProperty(REDIS_CLUSTER_NODES_CONFIG_PROPERTY)) {
			Object redisClusterNodes = propertySource.getProperty(REDIS_CLUSTER_NODES_CONFIG_PROPERTY);
			appendClusterNodes(commaDelimitedListToSet(String.valueOf(redisClusterNodes)));
		}
		if (propertySource.containsProperty(REDIS_CLUSTER_MAX_REDIRECTS_CONFIG_PROPERTY)) {
			Object clusterMaxRedirects = propertySource.getProperty(REDIS_CLUSTER_MAX_REDIRECTS_CONFIG_PROPERTY);
			this.maxRedirects = NumberUtils.parseNumber(String.valueOf(clusterMaxRedirects), Integer.class);
		}
	}

	private void appendClusterNodes(Set<String> hostAndPorts) {

		for (String hostAndPort : hostAndPorts) {
			addClusterNode(RedisNode.fromString(hostAndPort));
		}
	}

	/**
	 * Set {@literal cluster nodes} to connect to.
	 *
	 * @param nodes must not be {@literal null}.
	 */
	public void setClusterNodes(Iterable<RedisNode> nodes) {

		Assert.notNull(nodes, "Cannot set cluster nodes to null");

		this.clusterNodes.clear();

		for (RedisNode clusterNode : nodes) {
			addClusterNode(clusterNode);
		}
	}

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
		this.clusterNodes.add(RedisAssertions.requireNonNull(node, "ClusterNode must not be null"));
	}

	/**
	 * @param host Redis cluster node host name or ip address.
	 * @param port Redis cluster node port.
	 * @return this.
	 */
	public RedisClusterConfiguration clusterNode(String host, Integer port) {
		return clusterNode(new RedisNode(host, port));
	}

	/**
	 * @return this.
	 */
	public RedisClusterConfiguration clusterNode(RedisNode node) {

		this.clusterNodes.add(node);

		return this;
	}

	/**
	 * @param maxRedirects the max number of redirects to follow.
	 */
	public void setMaxRedirects(int maxRedirects) {

		Assert.isTrue(maxRedirects >= 0, "MaxRedirects must be greater or equal to 0");

		this.maxRedirects = maxRedirects;
	}

	@Override
	public Integer getMaxRedirects() {
		return maxRedirects != null && maxRedirects > Integer.MIN_VALUE ? maxRedirects : null;
	}

	@Override
	public void setUsername(@Nullable String username) {
		this.username = username;
	}

	@Nullable
	@Override
	public String getUsername() {
		return this.username;
	}

	@Override
	public void setPassword(RedisPassword password) {
		this.password = RedisAssertions.requireNonNull(password, "RedisPassword must not be null");
	}

	@Override
	public RedisPassword getPassword() {
		return password;
	}

	@Override
	public void setAsyncTaskExecutor(@Nullable AsyncTaskExecutor executor) {
		this.executor = executor;
	}

	@Nullable @Override
	public AsyncTaskExecutor getAsyncTaskExecutor() {
		return this.executor;
	}

	@Override
	public boolean equals(@Nullable Object obj) {

		if (this == obj) {
			return true;
		}

		if (!(obj instanceof RedisClusterConfiguration that)) {
			return false;
		}

		return ObjectUtils.nullSafeEquals(this.clusterNodes, that.clusterNodes)
			&& ObjectUtils.nullSafeEquals(this.maxRedirects, that.maxRedirects)
			&& ObjectUtils.nullSafeEquals(this.username, that.username)
			&& ObjectUtils.nullSafeEquals(this.password, that.password);
	}

	@Override
	public int hashCode() {
		int result = ObjectUtils.nullSafeHashCode(clusterNodes);
		result = 31 * result + ObjectUtils.nullSafeHashCode(maxRedirects);
		result = 31 * result + ObjectUtils.nullSafeHashCode(username);
		result = 31 * result + ObjectUtils.nullSafeHashCode(password);
		return result;
	}

	/**
	 * @param clusterHostAndPorts must not be {@literal null} or empty.
	 * @param redirects the max number of redirects to follow.
	 * @return cluster config map with properties.
	 */
	private static Map<String, Object> asMap(Collection<String> clusterHostAndPorts, int redirects) {

		Assert.notNull(clusterHostAndPorts, "ClusterHostAndPorts must not be null");
		Assert.noNullElements(clusterHostAndPorts, "ClusterHostAndPorts must not contain null elements");

		Map<String, Object> map = new HashMap<>();

		map.put(REDIS_CLUSTER_NODES_CONFIG_PROPERTY, StringUtils.collectionToCommaDelimitedString(clusterHostAndPorts));

		if (redirects >= 0) {
			map.put(REDIS_CLUSTER_MAX_REDIRECTS_CONFIG_PROPERTY, redirects);
		}

		return map;
	}
}
