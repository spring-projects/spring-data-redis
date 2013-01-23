/*
 * Copyright 2011-2013 the original author or authors.
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
package org.springframework.data.redis.connection;

import java.util.List;
import java.util.Set;


/**
 * Key-specific commands supported by Redis.
 * 
 * @author Costin Leau
 */
public interface RedisKeyCommands {

	Boolean exists(byte[] key);

	Long del(byte[]... keys);

	DataType type(byte[] key);

	Set<byte[]> keys(byte[] pattern);

	byte[] randomKey();

	void rename(byte[] oldName, byte[] newName);

	Boolean renameNX(byte[] oldName, byte[] newName);

	Boolean expire(byte[] key, long seconds);

	Boolean expireAt(byte[] key, long unixTime);

	Boolean persist(byte[] key);

	Boolean move(byte[] key, int dbIndex);

	Long ttl(byte[] key);

	// sort commands
	List<byte[]> sort(byte[] key, SortParameters params);

	Long sort(byte[] key, SortParameters params, byte[] storeKey);
}