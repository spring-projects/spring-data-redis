/*
 * Copyright 2014-2019 the original author or authors.
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

import static org.springframework.util.Assert.*;
import static org.springframework.util.StringUtils.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.PropertySource;
import org.springframework.util.StringUtils;

/**
 * Configuration class used for setting up {@link RedisConnection} via {@link RedisConnectionFactory} using connecting
 * to <a href="http://redis.io/topics/sentinel">Redis Sentinel(s)</a>. Useful when setting up a high availability Redis
 * environment.
 * 
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @since 1.4
 */
public class RedisSentinelConfiguration {

	private static final String REDIS_SENTINEL_MASTER_CONFIG_PROPERTY = "spring.redis.sentinel.master";
	private static final String REDIS_SENTINEL_NODES_CONFIG_PROPERTY = "spring.redis.sentinel.nodes";

	private NamedNode master;
	private Set<RedisNode> sentinels;

	/**
	 * Creates new {@link RedisSentinelConfiguration}.
	 */
	public RedisSentinelConfiguration() {
		this(new MapPropertySource("RedisSentinelConfiguration", Collections.<String, Object> emptyMap()));
	}

	/**
	 * Creates {@link RedisSentinelConfiguration} for given hostPort combinations.
	 * 
	 * <pre>
	 * sentinelHostAndPorts[0] = 127.0.0.1:23679
	 * sentinelHostAndPorts[1] = 127.0.0.1:23680
	 * ...
	 * 
	 * <pre>
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

		notNull(propertySource, "PropertySource must not be null!");

		this.sentinels = new LinkedHashSet<RedisNode>();

		if (propertySource.containsProperty(REDIS_SENTINEL_MASTER_CONFIG_PROPERTY)) {
			this.setMaster(propertySource.getProperty(REDIS_SENTINEL_MASTER_CONFIG_PROPERTY).toString());
		}

		if (propertySource.containsProperty(REDIS_SENTINEL_NODES_CONFIG_PROPERTY)) {
			appendSentinels(commaDelimitedListToSet(propertySource.getProperty(REDIS_SENTINEL_NODES_CONFIG_PROPERTY)
					.toString()));
		}
	}

	/**
	 * Set {@literal Sentinels} to connect to.
	 * 
	 * @param sentinels must not be {@literal null}.
	 */
	public void setSentinels(Iterable<RedisNode> sentinels) {

		notNull(sentinels, "Cannot set sentinels to 'null'.");

		this.sentinels.clear();

		for (RedisNode sentinel : sentinels) {
			addSentinel(sentinel);
		}
	}

	/**
	 * Returns an {@link Collections#unmodifiableSet(Set)} of {@literal Sentinels}.
	 * 
	 * @return {@link Set} of sentinels. Never {@literal null}.
	 */
	public Set<RedisNode> getSentinels() {
		return Collections.unmodifiableSet(sentinels);
	}

	/**
	 * Add sentinel.
	 * 
	 * @param sentinel must not be {@literal null}.
	 */
	public void addSentinel(RedisNode sentinel) {

		notNull(sentinel, "Sentinel must not be 'null'.");
		this.sentinels.add(sentinel);
	}

	/**
	 * Set the master node via its name.
	 * 
	 * @param name must not be {@literal null}.
	 */
	public void setMaster(final String name) {

		notNull(name, "Name of sentinel master must not be null.");
		setMaster(new NamedNode() {

			@Override
			public String getName() {
				return name;
			}
		});
	}

	/**
	 * Set the master.
	 * 
	 * @param master must not be {@literal null}.
	 */
	public void setMaster(NamedNode master) {

		notNull(master, "Sentinel master node must not be 'null'.");
		this.master = master;
	}

	/**
	 * Get the {@literal Sentinel} master node.
	 * 
	 * @return
	 */
	public NamedNode getMaster() {
		return master;
	}

	/**
	 * @see #setMaster(String)
	 * @param master
	 * @return
	 */
	public RedisSentinelConfiguration master(String master) {
		this.setMaster(master);
		return this;
	}

	/**
	 * @see #setMaster(NamedNode)
	 * @param master
	 * @return
	 */
	public RedisSentinelConfiguration master(NamedNode master) {
		this.setMaster(master);
		return this;
	}

	/**
	 * @see #addSentinel(RedisNode)
	 * @param sentinel
	 * @return
	 */
	public RedisSentinelConfiguration sentinel(RedisNode sentinel) {
		this.addSentinel(sentinel);
		return this;
	}

	/**
	 * @see #sentinel(RedisNode)
	 * @param host
	 * @param port
	 * @return
	 */
	public RedisSentinelConfiguration sentinel(String host, Integer port) {
		return sentinel(new RedisNode(host, port));
	}

	private void appendSentinels(Set<String> hostAndPorts) {

		for (String hostAndPort : hostAndPorts) {
			addSentinel(readHostAndPortFromString(hostAndPort));
		}
	}

	private RedisNode readHostAndPortFromString(String hostAndPort) {

		String[] args = split(hostAndPort, ":");

		notNull(args, "HostAndPort need to be seperated by  ':'.");
		isTrue(args.length == 2, "Host and Port String needs to specified as host:port");
		return new RedisNode(args[0], Integer.valueOf(args[1]).intValue());
	}

	/**
	 * @param master must not be {@literal null} or empty.
	 * @param sentinelHostAndPorts must not be {@literal null}.
	 * @return
	 */
	private static Map<String, Object> asMap(String master, Set<String> sentinelHostAndPorts) {

		hasText(master, "Master address must not be null or empty!");
		notNull(sentinelHostAndPorts, "SentinelHostAndPorts must not be null!");

		Map<String, Object> map = new HashMap<String, Object>();
		map.put(REDIS_SENTINEL_MASTER_CONFIG_PROPERTY, master);
		map.put(REDIS_SENTINEL_NODES_CONFIG_PROPERTY, StringUtils.collectionToCommaDelimitedString(sentinelHostAndPorts));

		return map;
	}
}
