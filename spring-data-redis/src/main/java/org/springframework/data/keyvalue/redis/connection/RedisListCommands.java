/*
 * Copyright 2010-2011 the original author or authors.
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

import java.util.List;

/**
 * List-specific commands supported by Redis.
 * 
 * @author Costin Leau
 */
public interface RedisListCommands {

	public enum POSITION {
		BEFORE, AFTER
	}

	Long rPush(byte[] key, byte[] value);
	
	Long lPush(byte[] key, byte[] value);

	Long rPushX(byte[] key, byte[] value);

	Long lPushX(byte[] key, byte[] value);

	Long lLen(byte[] key);

	List<byte[]> lRange(byte[] key, long start, long end);

	void lTrim(byte[] key, long start, long end);

	byte[] lIndex(byte[] key, long index);

	Long lInsert(byte[] key, POSITION where, byte[] pivot, byte[] value);

	void lSet(byte[] key, long index, byte[] value);

	Long lRem(byte[] key, long count, byte[] value);

	byte[] lPop(byte[] key);

	byte[] rPop(byte[] key);

	List<byte[]> bLPop(int timeout, byte[]... keys);

	List<byte[]> bRPop(int timeout, byte[]... keys);

	byte[] rPopLPush(byte[] srcKey, byte[] dstKey);

	byte[] bRPopLPush(int timeout, byte[] srcKey, byte[] dstKey);
}
