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
 * List-specific commands supported by Redis.
 * 
 * @author Costin Leau
 */
public interface RedisListCommands {

	Integer rPush(String key, String value);
	
	Integer lPush(String key, String value);

	Integer lLen(String key);

	List<String> lRange(String key, int start, int end);

	void lTrim(String key, int start, int end);

	String lIndex(String key, int index);

	void lSet(String key, int index, String value);

	Integer lRem(String key, int count, String value);

	String lPop(String key);

	String rPop(String key);

	List<String> bLPop(int timeout, String... keys);

	List<String> bRPop(int timeout, String... keys);

	String rPopLPush(String srcKey, String dstKey);
}
