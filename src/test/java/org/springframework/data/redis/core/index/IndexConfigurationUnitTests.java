/*
 * Copyright 2015-2025 the original author or authors.
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
package org.springframework.data.redis.core.index;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;

/**
 * @author Rob Winch
 * @author Christoph Strobl
 */
class IndexConfigurationUnitTests {

	@Test // DATAREDIS-425
	void redisIndexSettingIndexNameDefaulted() {

		String path = "path";
		SimpleIndexDefinition setting = new SimpleIndexDefinition("keyspace", path);
		assertThat(setting.getIndexName()).isEqualTo(path);
	}

	@Test // DATAREDIS-425
	void redisIndexSettingIndexNameExplicit() {

		String indexName = "indexName";
		SimpleIndexDefinition setting = new SimpleIndexDefinition("keyspace", "index", indexName);
		assertThat(setting.getIndexName()).isEqualTo(indexName);
	}

	@Test // DATAREDIS-425
	void redisIndexSettingIndexNameUsedInEquals() {

		SimpleIndexDefinition setting1 = new SimpleIndexDefinition("keyspace", "path", "indexName1");
		SimpleIndexDefinition setting2 = new SimpleIndexDefinition(setting1.getKeyspace(), "path",
				setting1.getIndexName() + "other");

		assertThat(setting1).isNotEqualTo(setting2);
	}

	@Test // DATAREDIS-425
	void redisIndexSettingIndexNameUsedInHashCode() {

		SimpleIndexDefinition setting1 = new SimpleIndexDefinition("keyspace", "path", "indexName1");
		SimpleIndexDefinition setting2 = new SimpleIndexDefinition(setting1.getKeyspace(), "path",
				setting1.getIndexName() + "other");

		assertThat(setting1.hashCode()).isNotEqualTo(setting2.hashCode());
	}
}
