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
 * Set-specific commands supported by Redis.
 * 
 * @author Costin Leau
 */
public interface RedisSetCommands {

	Boolean sAdd(byte[] key, byte[] value);

	Boolean sRem(byte[] key, byte[] value);

	byte[] sPop(byte[] key);

	Boolean sMove(byte[] srcKey, byte[] destKey, byte[] value);

	Long sCard(byte[] key);

	Boolean sIsMember(byte[] key, byte[] value);

	Set<byte[]> sInter(byte[]... keys);

	Long sInterStore(byte[] destKey, byte[]... keys);

	Set<byte[]> sUnion(byte[]... keys);

	Long sUnionStore(byte[] destKey, byte[]... keys);

	Set<byte[]> sDiff(byte[]... keys);

	Long sDiffStore(byte[] destKey, byte[]... keys);

	Set<byte[]> sMembers(byte[] key);

	byte[] sRandMember(byte[] key);

	List<byte[]> sRandMember(byte[] key, long count);
}
