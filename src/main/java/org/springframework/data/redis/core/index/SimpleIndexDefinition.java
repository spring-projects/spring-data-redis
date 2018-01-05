/*
 * Copyright 2016-2018 the original author or authors.
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

/**
 * {@link PathBasedRedisIndexDefinition} for including property values in a secondary index. <br />
 * Uses Redis {@literal SET} for storage. <br />
 *
 * @author Christoph Strobl
 * @since 1.7
 */
public class SimpleIndexDefinition extends RedisIndexDefinition implements PathBasedRedisIndexDefinition {

	/**
	 * Creates new {@link SimpleIndexDefinition}.
	 *
	 * @param keyspace must not be {@literal null}.
	 * @param path
	 */
	public SimpleIndexDefinition(String keyspace, String path) {
		this(keyspace, path, path);
	}

	/**
	 * Creates new {@link SimpleIndexDefinition}.
	 *
	 * @param keyspace must not be {@literal null}.
	 * @param path
	 * @param name must not be {@literal null}.
	 */
	public SimpleIndexDefinition(String keyspace, String path, String name) {
		super(keyspace, path, name);
		addCondition(new PathCondition(path));
	}

}
