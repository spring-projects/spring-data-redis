/*
 * Copyright 2014-2023 the original author or authors.
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

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

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

	private RedisNode(RedisNode redisNode) {

		this.id = redisNode.id;
		this.name = redisNode.name;
		this.host = redisNode.host;
		this.port = redisNode.port;
		this.type = redisNode.type;
		this.masterId = redisNode.masterId;
	}

	/**
	 * Parse a {@code hostAndPort} string into {@link RedisNode}. Supports IPv4, IPv6, and hostname notations including
	 * the port. For example:
	 *
	 * <pre class="code">
	 * RedisNode.fromString("127.0.0.1:6379");
	 * RedisNode.fromString("[aaaa:bbbb::dddd:eeee]:6379");
	 * RedisNode.fromString("my.redis.server:6379");
	 * </pre>
	 *
	 * @param hostPortString must not be {@literal null} or empty.
	 * @return the parsed {@link RedisNode}.
	 * @since 2.7.4
	 */
	public static RedisNode fromString(String hostPortString) {

		Assert.notNull(hostPortString, "HostAndPort must not be null");

		String host;
		String portString = null;

		if (hostPortString.startsWith("[")) {
			String[] hostAndPort = getHostAndPortFromBracketedHost(hostPortString);
			host = hostAndPort[0];
			portString = hostAndPort[1];
		} else {
			int colonPos = hostPortString.indexOf(':');
			if (colonPos >= 0 && hostPortString.indexOf(':', colonPos + 1) == -1) {
				// Exactly 1 colon. Split into host:port.
				host = hostPortString.substring(0, colonPos);
				portString = hostPortString.substring(colonPos + 1);
			} else {
				// 0 or 2+ colons. Bare hostname or IPv6 literal.
				host = hostPortString;
			}
		}

		int port = -1;
		try {
			port = Integer.parseInt(portString);
		} catch (RuntimeException e) {
			throw new IllegalArgumentException(String.format("Unparseable port number: %s", hostPortString));
		}

		if (!isValidPort(port)) {
			throw new IllegalArgumentException(String.format("Port number out of range: %s", hostPortString));
		}

		return new RedisNode(host, port);
	}

	/**
	 * Parses a bracketed host-port string, throwing IllegalArgumentException if parsing fails.
	 *
	 * @param hostPortString the full bracketed host-port specification. Post might not be specified.
	 * @return an array with 2 strings: host and port, in that order.
	 * @throws IllegalArgumentException if parsing the bracketed host-port string fails.
	 */
	private static String[] getHostAndPortFromBracketedHost(String hostPortString) {

		if (hostPortString.charAt(0) != '[') {
			throw new IllegalArgumentException(
					String.format("Bracketed host-port string must start with a bracket: %s", hostPortString));
		}

		int colonIndex = hostPortString.indexOf(':');
		int closeBracketIndex = hostPortString.lastIndexOf(']');

		if (!(colonIndex > -1 && closeBracketIndex > colonIndex)) {
			throw new IllegalArgumentException(String.format("Invalid bracketed host/port: %s", hostPortString));
		}

		String host = hostPortString.substring(1, closeBracketIndex);
		if (closeBracketIndex + 1 == hostPortString.length()) {
			return new String[] { host, "" };
		} else {
			if (!(hostPortString.charAt(closeBracketIndex + 1) == ':')) {
				throw new IllegalArgumentException(
						String.format("Only a colon may follow a close bracket: %s", hostPortString));
			}
			for (int i = closeBracketIndex + 2; i < hostPortString.length(); ++i) {
				if (!Character.isDigit(hostPortString.charAt(i))) {
					throw new IllegalArgumentException(String.format("Port must be numeric: %s", hostPortString));
				}
			}
			return new String[] { host, hostPortString.substring(closeBracketIndex + 2) };
		}
	}

	/**
	 * @return can be {@literal null}.
	 */
	@Nullable
	public String getHost() {
		return host;
	}

	/**
	 * @return whether this node has a valid host (not null and not empty).
	 * @since 2.3.8
	 */
	public boolean hasValidHost() {
		return StringUtils.hasText(host);
	}

	/**
	 * @return can be {@literal null}.
	 */
	@Nullable
	public Integer getPort() {
		return port;
	}

	public String asString() {

		if (host != null && host.contains(":")) {
			return "[" + host + "]:" + port;
		}

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
		return isReplica();
	}

	/**
	 * @return
	 * @since 2.1
	 */
	public boolean isReplica() {
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
	public boolean equals(@Nullable Object obj) {

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

			Assert.notNull(host, "Hostname must not be null.");
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
		 * @param type
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
			return replicaOf(masterId);
		}

		/**
		 * Set the id of the master node.
		 *
		 * @param masterId
		 * @return this.
		 * @since 2.1
		 */
		public RedisNodeBuilder replicaOf(String masterId) {

			node.masterId = masterId;
			return this;
		}

		/**
		 * Get the {@link RedisNode}.
		 *
		 * @return
		 */
		public RedisNode build() {
			return new RedisNode(this.node);
		}
	}

	private static boolean isValidPort(int port) {
		return port >= 0 && port <= 65535;
	}

}
