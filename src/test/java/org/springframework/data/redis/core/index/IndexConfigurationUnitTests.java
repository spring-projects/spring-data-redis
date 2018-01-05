/*
 * Copyright 2015-2018 the original author or authors.
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

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.junit.Test;

/**
 * @author Rob Winch
 * @author Christoph Strobl
 */
public class IndexConfigurationUnitTests {

	@Test // DATAREDIS-425
	public void redisIndexSettingIndexNameDefaulted() {

		String path = "path";
		SimpleIndexDefinition setting = new SimpleIndexDefinition("keyspace", path);
		assertThat(setting.getIndexName(), equalTo(path));
	}

	@Test // DATAREDIS-425
	public void redisIndexSettingIndexNameExplicit() {

		String indexName = "indexName";
		SimpleIndexDefinition setting = new SimpleIndexDefinition("keyspace", "index", indexName);
		assertThat(setting.getIndexName(), equalTo(indexName));
	}

	@Test // DATAREDIS-425
	public void redisIndexSettingIndexNameUsedInEquals() {

		SimpleIndexDefinition setting1 = new SimpleIndexDefinition("keyspace", "path", "indexName1");
		SimpleIndexDefinition setting2 = new SimpleIndexDefinition(setting1.getKeyspace(), "path", setting1.getIndexName()
				+ "other");

		assertThat(setting1, not(equalTo(setting2)));
	}

	@Test // DATAREDIS-425
	public void redisIndexSettingIndexNameUsedInHashCode() {

		SimpleIndexDefinition setting1 = new SimpleIndexDefinition("keyspace", "path", "indexName1");
		SimpleIndexDefinition setting2 = new SimpleIndexDefinition(setting1.getKeyspace(), "path", setting1.getIndexName()
				+ "other");

		assertThat(setting1.hashCode(), not(equalTo(setting2.hashCode())));
	}
}
