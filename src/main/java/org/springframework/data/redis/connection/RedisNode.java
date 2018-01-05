/*
 * Copyright 2014-2018 the original author or authors.
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

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Mark Paluch
 * @since 1.4
 */
public class RedisNode implements NamedNode {

	@Nullable String id;
	@Nullable String name;
	@Nullable String host;
	int port;
	@Nullable NodeType type;
	@Nullable String masterId;

	/**
	 * Creates a new {@link RedisNode} with the given {@code host}, {@code port}.
	 *
	 * @param host must not be {@literal null}
	 * @param port
	 */
	public RedisNode(String host, int port) {

		Assert.notNull(host, "host must not be null!");

		this.host = host;
		this.port = port;
	}

	protected RedisNode() {}

	/**
	 * @return can be {@literal null}.
	 */
	@Nullable
	public String getHost() {
		return host;
	}

	/**
	 * @return can be {@literal null}.
	 */
	@Nullable
	public Integer getPort() {
		return port;
	}

	public String asString() {
		return host + ":" + port;
	}

	@Override
	@Nullable
	public String getName() {
		return this.name;
	}

	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return can be {@literal null}.
	 * @since 1.7
	 */
	@Nullable
	public String getMasterId() {
		return masterId;
	}

	/**
	 * @return can be {@literal null}.
	 * @since 1.7
	 */
	@Nullable
	public String getId() {
		return id;
	}

	/**
	 * @param id
	 * @since 1.7
	 */
	public void setId(String id) {
		this.id = id;
	}

	/**
	 * @return can be {@literal null}.
	 * @since 1.7
	 */
	@Nullable
	public NodeType getType() {
		return type;
	}

	/**
	 * @return
	 * @since 1.7
	 */
	public boolean isMaster() {
		return ObjectUtils.nullSafeEquals(NodeType.MASTER, getType());
	}

	/**
	 * @return
	 * @since 1.7
	 */
	public boolean isSlave() {
		return ObjectUtils.nullSafeEquals(NodeType.SLAVE, getType());
	}

	/**
	 * Get {@link RedisNodeBuilder} for creating new {@link RedisNode}.
	 *
	 * @return never {@literal null}.
	 * @since 1.7
	 */
	public static RedisNodeBuilder newRedisNode() {
		return new RedisNodeBuilder();
	}

	@Override
	public String toString() {
		return asString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ObjectUtils.nullSafeHashCode(host);
		result = prime * result + ObjectUtils.nullSafeHashCode(port);
		return result;
	}

	@Override
	public boolean equals(Object obj) {

		if (this == obj) {
			return true;
		}
		if (obj == null || !(obj instanceof RedisNode)) {
			return false;
		}

		RedisNode other = (RedisNode) obj;

		if (!ObjectUtils.nullSafeEquals(this.host, other.host)) {
			return false;
		}

		if (!ObjectUtils.nullSafeEquals(this.port, other.port)) {
			return false;
		}

		if (!ObjectUtils.nullSafeEquals(this.name, other.name)) {
			return false;
		}

		return true;
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	public enum NodeType {
		MASTER, SLAVE
	}

	/**
	 * Builder for creating new {@link RedisNode}.
	 *
	 * @author Christoph Strobl
	 * @since 1.4
	 */
	public static class RedisNodeBuilder {

		private RedisNode node;

		public RedisNodeBuilder() {
			node = new RedisNode();
		}

		/**
		 * Define node name.
		 */
		public RedisNodeBuilder withName(String name) {
			node.name = name;
			return this;
		}

		/**
		 * Set host and port of server.
		 *
		 * @param host must not be {@literal null}.
		 * @param port
		 * @return
		 */
		public RedisNodeBuilder listeningAt(String host, int port) {

			Assert.hasText(host, "Hostname must not be empty or null.");
			node.host = host;
			node.port = port;
			return this;
		}

		/**
		 * Set id of server.
		 *
		 * @param id
		 * @return
		 */
		public RedisNodeBuilder withId(String id) {

			node.id = id;
			return this;
		}

		/**
		 * Set server role.
		 *
		 * @param nodeType
		 * @return
		 * @since 1.7
		 */
		public RedisNodeBuilder promotedAs(NodeType type) {

			node.type = type;
			return this;
		}

		/**
		 * Set the id of the master node.
		 *
		 * @param masterId
		 * @return
		 * @since 1.7
		 */
		public RedisNodeBuilder slaveOf(String masterId) {

			node.masterId = masterId;
			return this;
		}

		/**
		 * Get the {@link RedisNode}.
		 *
		 * @return
		 */
		public RedisNode build() {
			return this.node;
		}
	}

}
