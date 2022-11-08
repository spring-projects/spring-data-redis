/*
 * Copyright 2014-2022 the original author or authors.
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

import static org.springframework.util.StringUtils.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.PropertySource;
import org.springframework.data.redis.connection.RedisConfiguration.SentinelConfiguration;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * Configuration class used for setting up {@link RedisConnection} via {@link RedisConnectionFactory} using connecting
 * to <a href="https://redis.io/topics/sentinel">Redis Sentinel(s)</a>. Useful when setting up a high availability Redis
 * environment.
 *
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Mark Paluch
 * @author Vikas Garg
 * @since 1.4
 */
public class RedisSentinelConfiguration implements RedisConfiguration, SentinelConfiguration {

	private static final String REDIS_SENTINEL_MASTER_CONFIG_PROPERTY = "spring.redis.sentinel.master";
	private static final String REDIS_SENTINEL_NODES_CONFIG_PROPERTY = "spring.redis.sentinel.nodes";
	private static final String REDIS_SENTINEL_USERNAME_CONFIG_PROPERTY = "spring.redis.sentinel.username";
	private static final String REDIS_SENTINEL_PASSWORD_CONFIG_PROPERTY = "spring.redis.sentinel.password";

	private @Nullable NamedNode master;
	private Set<RedisNode> sentinels;
	private int database;

	private @Nullable String dataNodeUsername = null;
	private @Nullable String sentinelUsername = null;
	private RedisPassword dataNodePassword = RedisPassword.none();
	private RedisPassword sentinelPassword = RedisPassword.none();

	/**
	 * Creates new {@link RedisSentinelConfiguration}.
	 */
	public RedisSentinelConfiguration() {
		this(new MapPropertySource("RedisSentinelConfiguration", Collections.emptyMap()));
	}

	/**
	 * Creates {@link RedisSentinelConfiguration} for given hostPort combinations.
	 *
	 * <pre>
	 * sentinelHostAndPorts[0] = 127.0.0.1:23679 sentinelHostAndPorts[1] = 127.0.0.1:23680 ...
	 * </pre>
	 *
	 * @param sentinelHostAndPorts must not be {@literal null}.
	 * @since 1.5
	 */
	public RedisSentinelConfiguration(String master, Set<String> sentinelHostAndPorts) {
		this(new MapPropertySource("RedisSentinelConfiguration", asMap(master, sentinelHostAndPorts)));
	}

	/**
	 * Creates {@link RedisSentinelConfiguration} looking up values in given {@link PropertySource}.
	 *
	 * <pre>
	 * <code>
	 * spring.redis.sentinel.master=myMaster
	 * spring.redis.sentinel.nodes=127.0.0.1:23679,127.0.0.1:23680,127.0.0.1:23681
	 * </code>
	 * </pre>
	 *
	 * @param propertySource must not be {@literal null}.
	 * @since 1.5
	 */
	public RedisSentinelConfiguration(PropertySource<?> propertySource) {

		Assert.notNull(propertySource, "PropertySource must not be null");

		this.sentinels = new LinkedHashSet<>();

		if (propertySource.containsProperty(REDIS_SENTINEL_MASTER_CONFIG_PROPERTY)) {
			this.setMaster(propertySource.getProperty(REDIS_SENTINEL_MASTER_CONFIG_PROPERTY).toString());
		}

		if (propertySource.containsProperty(REDIS_SENTINEL_NODES_CONFIG_PROPERTY)) {
			appendSentinels(
					commaDelimitedListToSet(propertySource.getProperty(REDIS_SENTINEL_NODES_CONFIG_PROPERTY).toString()));
		}

		if (propertySource.containsProperty(REDIS_SENTINEL_PASSWORD_CONFIG_PROPERTY)) {
			this.setSentinelPassword(propertySource.getProperty(REDIS_SENTINEL_PASSWORD_CONFIG_PROPERTY).toString());
		}

		if (propertySource.containsProperty(REDIS_SENTINEL_USERNAME_CONFIG_PROPERTY)) {
			this.setSentinelUsername(propertySource.getProperty(REDIS_SENTINEL_USERNAME_CONFIG_PROPERTY).toString());
		}
	}

	/**
	 * Set {@literal Sentinels} to connect to.
	 *
	 * @param sentinels must not be {@literal null}.
	 */
	public void setSentinels(Iterable<RedisNode> sentinels) {

		Assert.notNull(sentinels, "Cannot set sentinels to 'null'");

		this.sentinels.clear();

		for (RedisNode sentinel : sentinels) {
			addSentinel(sentinel);
		}
	}

	public Set<RedisNode> getSentinels() {
		return Collections.unmodifiableSet(sentinels);
	}

	/**
	 * Add sentinel.
	 *
	 * @param sentinel must not be {@literal null}.
	 */
	public void addSentinel(RedisNode sentinel) {

		Assert.notNull(sentinel, "Sentinel must not be 'null'");
		this.sentinels.add(sentinel);
	}

	public void setMaster(NamedNode master) {

		Assert.notNull(master, "Sentinel master node must not be 'null'");
		this.master = master;
	}

	public NamedNode getMaster() {
		return master;
	}

	/**
	 * @see #setMaster(String)
	 * @param master The master node name.
	 * @return this.
	 */
	public RedisSentinelConfiguration master(String master) {
		this.setMaster(master);
		return this;
	}

	/**
	 * @see #setMaster(NamedNode)
	 * @param master the master node
	 * @return this.
	 */
	public RedisSentinelConfiguration master(NamedNode master) {
		this.setMaster(master);
		return this;
	}

	/**
	 * @see #addSentinel(RedisNode)
	 * @param sentinel the node to add as sentinel.
	 * @return this.
	 */
	public RedisSentinelConfiguration sentinel(RedisNode sentinel) {
		this.addSentinel(sentinel);
		return this;
	}

	/**
	 * @see #sentinel(RedisNode)
	 * @param host redis sentinel node host name or ip.
	 * @param port redis sentinel port.
	 * @return this.
	 */
	public RedisSentinelConfiguration sentinel(String host, Integer port) {
		return sentinel(new RedisNode(host, port));
	}

	private void appendSentinels(Set<String> hostAndPorts) {

		for (String hostAndPort : hostAndPorts) {
			addSentinel(RedisNode.fromString(hostAndPort));
		}
	}

	@Override
	public int getDatabase() {
		return database;
	}

	@Override
	public void setDatabase(int index) {

		Assert.isTrue(index >= 0, () -> String.format("Invalid DB index '%s' (a positive index required)", index));

		this.database = index;
	}

	@Override
	public void setUsername(@Nullable String username) {
		this.dataNodeUsername = username;
	}

	@Nullable
	@Override
	public String getUsername() {
		return this.dataNodeUsername;
	}

	@Override
	public RedisPassword getPassword() {
		return dataNodePassword;
	}

	@Override
	public void setPassword(RedisPassword password) {

		Assert.notNull(password, "RedisPassword must not be null");

		this.dataNodePassword = password;
	}

	@Nullable
	@Override
	public String getSentinelUsername() {
		return this.sentinelUsername;
	}

	@Override
	public void setSentinelUsername(@Nullable String sentinelUsername) {
		this.sentinelUsername = sentinelUsername;
	}

	@Override
	public void setSentinelPassword(RedisPassword sentinelPassword) {

		Assert.notNull(sentinelPassword, "SentinelPassword must not be null");
		this.sentinelPassword = sentinelPassword;
	}

	@Override
	public RedisPassword getSentinelPassword() {
		return sentinelPassword;
	}

	@Override
	public boolean equals(@Nullable Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof RedisSentinelConfiguration)) {
			return false;
		}
		RedisSentinelConfiguration that = (RedisSentinelConfiguration) o;
		if (database != that.database) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(master, that.master)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(sentinels, that.sentinels)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(dataNodeUsername, that.dataNodeUsername)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(dataNodePassword, that.dataNodePassword)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(sentinelUsername, that.sentinelUsername)) {
			return false;
		}
		return ObjectUtils.nullSafeEquals(sentinelPassword, that.sentinelPassword);
	}

	@Override
	public int hashCode() {
		int result = ObjectUtils.nullSafeHashCode(master);
		result = 31 * result + ObjectUtils.nullSafeHashCode(sentinels);
		result = 31 * result + database;
		result = 31 * result + ObjectUtils.nullSafeHashCode(dataNodeUsername);
		result = 31 * result + ObjectUtils.nullSafeHashCode(dataNodePassword);
		result = 31 * result + ObjectUtils.nullSafeHashCode(sentinelUsername);
		result = 31 * result + ObjectUtils.nullSafeHashCode(sentinelPassword);
		return result;
	}

	/**
	 * @param master must not be {@literal null} or empty.
	 * @param sentinelHostAndPorts must not be {@literal null}.
	 * @return configuration map.
	 */
	private static Map<String, Object> asMap(String master, Set<String> sentinelHostAndPorts) {

		Assert.hasText(master, "Master address must not be null or empty");
		Assert.notNull(sentinelHostAndPorts, "SentinelHostAndPorts must not be null");
		Assert.noNullElements(sentinelHostAndPorts, "ClusterHostAndPorts must not contain null elements");

		Map<String, Object> map = new HashMap<>();
		map.put(REDIS_SENTINEL_MASTER_CONFIG_PROPERTY, master);
		map.put(REDIS_SENTINEL_NODES_CONFIG_PROPERTY, StringUtils.collectionToCommaDelimitedString(sentinelHostAndPorts));

		return map;
	}
}
