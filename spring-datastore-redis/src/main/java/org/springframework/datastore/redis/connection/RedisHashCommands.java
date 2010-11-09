/*
 * Copyright 2010 the original author or authors.
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

package org.springframework.datastore.redis.connection;

import java.util.List;
import java.util.Set;

/**
 * Hash-specific commands supported by Redis.
 * 
 * @author Costin Leau
 */
public interface RedisHashCommands {

	public interface Entry {
		public String getField();

		public String getValue();
	}

	Boolean hSet(String key, String field, String value);

	Boolean hSetNX(String key, String field, String value);

	String hGet(String key, String field);

	List<String> hMGet(String key, String... fields);

	void hMSet(String key, String[] fields, String[] values);

	Integer hIncrBy(String key, String field, int delta);

	Boolean hExists(String key, String field);

	Boolean hDel(String key, String field);

	Integer hLen(String key);

	Set<String> hKeys(String key);

	List<String> hVals(String key);

	Set<Entry> hGetAll(String key);
}
