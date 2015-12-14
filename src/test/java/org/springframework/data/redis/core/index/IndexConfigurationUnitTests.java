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

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.junit.Test;
import org.springframework.data.redis.core.index.IndexConfiguration.RedisIndexSetting;

/**
 * @author Rob Winch
 */
public class IndexConfigurationUnitTests {

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void redisIndexSettingIndexNameDefaulted() {

		String path = "path";
		RedisIndexSetting setting = new RedisIndexSetting("keyspace", path);
		assertThat(setting.getIndexName(), equalTo(path));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void redisIndexSettingIndexNameExplicit() {

		String indexName = "indexName";
		RedisIndexSetting setting = new RedisIndexSetting("keyspace", "index", indexName, IndexType.SIMPLE);
		assertThat(setting.getIndexName(), equalTo(indexName));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void redisIndexSettingIndexNameUsedInEquals() {

		RedisIndexSetting setting1 = new RedisIndexSetting("keyspace", "path", "indexName1", IndexType.SIMPLE);
		RedisIndexSetting setting2 = new RedisIndexSetting(setting1.getKeyspace(), setting1.getPath(),
				setting1.getIndexName() + "other", setting1.getType());

		assertThat(setting1, not(equalTo(setting2)));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void redisIndexSettingIndexNameUsedInHashCode() {

		RedisIndexSetting setting1 = new RedisIndexSetting("keyspace", "path", "indexName1", IndexType.SIMPLE);
		RedisIndexSetting setting2 = new RedisIndexSetting(setting1.getKeyspace(), setting1.getPath(),
				setting1.getIndexName() + "other", setting1.getType());

		assertThat(setting1.hashCode(), not(equalTo(setting2.hashCode())));
	}
}
