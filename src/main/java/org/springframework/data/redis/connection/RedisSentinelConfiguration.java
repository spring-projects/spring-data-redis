/*
 * Copyright 2014 the original author or authors.
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

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import org.springframework.util.Assert;

/**
 * Configuration class used for setting up {@link RedisConnection} via {@link RedisConnectionFactory} using connecting
 * to <a href="http://redis.io/topics/sentinel">Redis Sentinel(s)</a>. Useful when setting up a high availability Redis
 * environment.
 * 
 * @author Christoph Strobl
 * @since 1.4
 */
public class RedisSentinelConfiguration {

	private NamedNode master;
	private Set<RedisNode> sentinels;

	/**
	 * Creates new {@link RedisSentinelConfiguration}.
	 */
	public RedisSentinelConfiguration() {
		this.sentinels = new LinkedHashSet<RedisNode>();
	}

	/**
	 * Set {@literal Sentinels} to connect to.
	 * 
	 * @param sentinels must not be {@literal null}.
	 */
	public void setSentinels(Iterable<RedisNode> sentinels) {

		Assert.notNull(sentinels, "Cannot set sentinels to 'null'.");
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

		Assert.notNull(sentinel, "Sentinel must not be 'null'.");
		this.sentinels.add(sentinel);
	}

	/**
	 * Set the master node via its name.
	 * 
	 * @param name must not be {@literal null}.
	 */
	public void setMaster(final String name) {

		Assert.notNull(name, "Name of sentinel master must not be null.");
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

		Assert.notNull("Sentinel master node must not be 'null'.");
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
}
