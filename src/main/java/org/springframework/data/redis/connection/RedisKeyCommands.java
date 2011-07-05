/*
 * Copyright 2011 the original author or authors.
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

	public abstract Boolean exists(byte[] key);

	public abstract Long del(byte[]... keys);

	public abstract DataType type(byte[] key);

	public abstract Set<byte[]> keys(byte[] pattern);

	public abstract byte[] randomKey();

	public abstract void rename(byte[] oldName, byte[] newName);

	public abstract Boolean renameNX(byte[] oldName, byte[] newName);

	public abstract Boolean expire(byte[] key, long seconds);

	public abstract Boolean expireAt(byte[] key, long unixTime);

	public abstract Boolean persist(byte[] key);

	public abstract Boolean move(byte[] key, int dbIndex);

	public abstract Long ttl(byte[] key);

	// sort commands
	public abstract List<byte[]> sort(byte[] key, SortParameters params);

	public abstract Long sort(byte[] key, SortParameters params, byte[] storeKey);

}