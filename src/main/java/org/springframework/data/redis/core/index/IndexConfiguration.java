/*
 * Copyright 2015-2016 the original author or authors.
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * {@link IndexConfiguration} allows programmatic setup of indexes. This is suitable for cases where there is no option
 * to use the equivalent {@link Indexed} annotation.
 *
 * @author Christoph Strobl
 * @author Rob Winch
 * @since 1.7
 */
public class IndexConfiguration {

	private final Set<RedisIndexSetting> definitions;

	/**
	 * Creates new empty {@link IndexConfiguration}.
	 */
	public IndexConfiguration() {

		this.definitions = new CopyOnWriteArraySet<RedisIndexSetting>();
		for (RedisIndexSetting initial : initialConfiguration()) {
			addIndexDefinition(initial);
		}
	}

	/**
	 * Checks if an index is defined for a given keyspace and property path.
	 *
	 * @param keyspace
	 * @param path
	 * @return true if index is defined.
	 */
	public boolean hasIndexFor(Serializable keyspace, String path) {

		for (IndexType type : IndexType.values()) {
			RedisIndexSetting def = getIndexDefinition(keyspace, path, type);
			if (def != null) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Get the list of {@link RedisIndexSetting} for a given keyspace and property path.
	 *
	 * @param keyspace
	 * @param path
	 * @return never {@literal null}.
	 */
	public List<RedisIndexSetting> getIndexDefinitionsFor(Serializable keyspace, String path) {

		List<RedisIndexSetting> indexDefinitions = new ArrayList<RedisIndexSetting>();
		for (IndexType type : IndexType.values()) {
			RedisIndexSetting def = getIndexDefinition(keyspace, path, type);
			if (def != null) {
				indexDefinitions.add(def);
			}
		}

		return indexDefinitions;
	}

	/**
	 * Gets all of the {@link RedisIndexSetting} for a given keyspace.
	 *
	 * @param keyspace the keyspace to get
	 * @return never {@literal null}
	 */
	public List<RedisIndexSetting> getIndexDefinitionsFor(Serializable keyspace) {

		List<RedisIndexSetting> indexDefinitions = new ArrayList<RedisIndexSetting>();

		for (RedisIndexSetting indexDef : definitions) {
			if (indexDef.getKeyspace().equals(keyspace)) {
				indexDefinitions.add(indexDef);
			}
		}

		return indexDefinitions;
	}

	/**
	 * Add given {@link RedisIndexSetting}.
	 *
	 * @param indexDefinition must not be {@literal null}.
	 */
	public void addIndexDefinition(RedisIndexSetting indexDefinition) {

		Assert.notNull(indexDefinition, "RedisIndexDefinition must not be null in order to be added.");
		this.definitions.add(indexDefinition);
	}

	private RedisIndexSetting getIndexDefinition(Serializable keyspace, String path, IndexType type) {

		for (RedisIndexSetting indexDef : definitions) {
			if (indexDef.getKeyspace().equals(keyspace) && indexDef.getPath().equals(path) && indexDef.getType().equals(type)) {
				return indexDef;
			}
		}

		return null;
	}

	/**
	 * Customization hook.
	 *
	 * @return must not return {@literal null}.
	 */
	protected Iterable<RedisIndexSetting> initialConfiguration() {
		return Collections.emptySet();
	}

	/**
	 * @author Christoph Strobl
	 * @author Rob Winch
	 * @since 1.7
	 */
	public static class RedisIndexSetting {

		private final Serializable keyspace;
		private final String path;
		private final String indexName;
		private final IndexType type;

		public RedisIndexSetting(Serializable keyspace, String path) {
			this(keyspace, path, null);
		}

		public RedisIndexSetting(Serializable keyspace, String path, IndexType type) {
			this(keyspace, path, path, type);
		}

		public RedisIndexSetting(Serializable keyspace, String path, String indexName, IndexType type) {
			this.keyspace = keyspace;
			this.path = path;
			this.indexName = indexName;
			this.type = type == null ? IndexType.SIMPLE : type;
		}

		public String getIndexName() {
			return indexName;
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
			result += ObjectUtils.nullSafeHashCode(indexName);
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
			if (!(obj instanceof RedisIndexSetting)) {
				return false;
			}

			RedisIndexSetting that = (RedisIndexSetting) obj;

			if (!ObjectUtils.nullSafeEquals(this.keyspace, that.keyspace)) {
				return false;
			}
			if (!ObjectUtils.nullSafeEquals(this.path, that.path)) {
				return false;
			}
			if (!ObjectUtils.nullSafeEquals(this.indexName, that.indexName)) {
				return false;
			}
			if (!ObjectUtils.nullSafeEquals(this.type, that.type)) {
				return false;
			}
			return true;
		}

	}

}
