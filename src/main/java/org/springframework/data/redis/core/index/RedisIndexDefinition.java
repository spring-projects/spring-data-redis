/*
 * Copyright 2015 the original author or authors.
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
package org.springframework.data.redis.core.index;

import java.io.Serializable;

import org.springframework.util.ObjectUtils;

/**
 * @author Christoph Strobl
 */
public class RedisIndexDefinition {

	private final Serializable keyspace;
	private final String path;
	private final IndexType type;

	public RedisIndexDefinition(Serializable keyspace, String path) {
		this(keyspace, path, null);
	}

	public RedisIndexDefinition(Serializable keyspace, String path, IndexType type) {

		this.keyspace = keyspace;
		this.path = path;
		this.type = type == null ? IndexType.SIMPLE : type;
	}

	public Serializable getKeyspace() {
		return keyspace;
	}

	public String getPath() {
		return path;
	}

	public IndexType getType() {
		return type;
	}

	@Override
	public int hashCode() {

		int result = ObjectUtils.nullSafeHashCode(keyspace);
		result += ObjectUtils.nullSafeHashCode(path);
		result += ObjectUtils.nullSafeHashCode(type);
		return result;
	}

	@Override
	public boolean equals(Object obj) {

		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof RedisIndexDefinition)) {
			return false;
		}

		RedisIndexDefinition that = (RedisIndexDefinition) obj;

		if (!ObjectUtils.nullSafeEquals(this.keyspace, that.keyspace)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(this.path, that.path)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(this.type, that.type)) {
			return false;
		}
		return true;
	}

}
