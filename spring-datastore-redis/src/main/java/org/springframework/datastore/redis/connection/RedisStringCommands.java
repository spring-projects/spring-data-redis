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

/**
 * String specific commands supported by Redis.
 * 
 * @author Costin Leau
 */
public interface RedisStringCommands {

	void set(String key, String value);

	String get(String key);

	String getSet(String key, String value);

	List<String> mGet(String... keys);

	Boolean setNX(String key, String value);

	void setEx(String key, int seconds, String value);

	void mSet(String[] keys, String[] values);

	void mSetNX(String[] keys, String[] values);

	Integer incr(String key);

	Integer incrBy(String key, int value);

	Integer decr(String key);

	Integer decrBy(String key, int value);

	Integer append(String key, String value);

	String substr(String key, int start, int end);
}
