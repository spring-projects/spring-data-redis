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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 */
public class IndexConfiguration {

	private final Set<RedisIndexDefinition> definitions;

	public IndexConfiguration() {
		this.definitions = new CopyOnWriteArraySet<RedisIndexDefinition>();
		for (RedisIndexDefinition initial : initialConfiguration()) {
			addIndexDefinition(initial);
		}
	}

	public boolean hasIndexFor(Serializable keyspace, String path) {

		for (IndexType type : IndexType.values()) {
			RedisIndexDefinition def = getIndexDefinition(keyspace, path, type);
			if (def != null) {
				return true;
			}
		}
		return false;
	}

	private RedisIndexDefinition getIndexDefinition(Serializable keyspace, String path, IndexType type) {

		for (RedisIndexDefinition indexDef : definitions) {
			if (indexDef.getKeyspace().equals(keyspace) && indexDef.getPath().equals(path) && indexDef.getType().equals(type)) {
				return indexDef;
			}
		}

		return null;
	}

	public List<RedisIndexDefinition> getIndexDefinitionsFor(Serializable keyspace, String path) {

		List<RedisIndexDefinition> indexDefinitions = new ArrayList<RedisIndexDefinition>();
		for (IndexType type : IndexType.values()) {
			RedisIndexDefinition def = getIndexDefinition(keyspace, path, type);
			if (def != null) {
				indexDefinitions.add(def);
			}
		}

		return indexDefinitions;
	}

	public void addIndexDefinition(RedisIndexDefinition indexDefinition) {

		Assert.notNull(indexDefinition, "RedisIndexDefinition must not be null in order to be added.");
		this.definitions.add(indexDefinition);
	}

	/**
	 * Customization hook.
	 * 
	 * @return must not return {@literal null}.
	 */
	protected Iterable<RedisIndexDefinition> initialConfiguration() {
		return Collections.emptySet();
	}

}
