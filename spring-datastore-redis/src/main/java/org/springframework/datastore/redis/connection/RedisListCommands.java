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

	Integer rPush(byte[] key, byte[] value);
	
	Integer lPush(byte[] key, byte[] value);

	Integer lLen(byte[] key);

	List<byte[]> lRange(byte[] key, int start, int end);

	void lTrim(byte[] key, int start, int end);

	byte[] lIndex(byte[] key, int index);

	void lSet(byte[] key, int index, byte[] value);

	Integer lRem(byte[] key, int count, byte[] value);

	byte[] lPop(byte[] key);

	byte[] rPop(byte[] key);

	List<byte[]> bLPop(int timeout, byte[]... keys);

	List<byte[]> bRPop(int timeout, byte[]... keys);

	byte[] rPopLPush(byte[] srcKey, byte[] dstKey);
}
