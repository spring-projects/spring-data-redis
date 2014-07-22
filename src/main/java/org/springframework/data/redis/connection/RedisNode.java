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

import org.springframework.util.ObjectUtils;

/**
 * @author Christoph Strobl
 * @since 1.4
 */
public class RedisNode {

	private final String host;
	private final int port;

	public RedisNode(String host, int port) {
		this.host = host;
		this.port = port;
	}

	public String getHost() {
		return host;
	}

	public Integer getPort() {
		return port;
	}

	public String asString() {
		return host + ":" + port;
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

		return true;
	}

}
