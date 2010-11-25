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

package org.springframework.data.keyvalue.redis.connection;

import java.util.Collection;

/**
 * Commands supported by Redis .
 * 
 * @author Costin Leau
 */
public interface RedisCommands extends RedisTxCommands, RedisStringCommands, RedisListCommands, RedisSetCommands,
		RedisZSetCommands, RedisHashCommands {

	Boolean exists(byte[] key);

	Integer del(byte[]... keys);

	DataType type(byte[] key);

	Collection<byte[]> keys(byte[] pattern);

	byte[] randomKey();

	void rename(byte[] oldName, byte[] newName);

	Boolean renameNX(byte[] oldName, byte[] newName);

	Integer dbSize();

	Boolean expire(byte[] key, int seconds);

	Boolean persist(byte[] key);

	Integer ttl(byte[] key);

	void select(int dbIndex);

	void flushDb();
}