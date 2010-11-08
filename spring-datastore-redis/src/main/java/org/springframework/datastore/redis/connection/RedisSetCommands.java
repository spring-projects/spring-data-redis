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

import java.util.Set;

/**
 * Set-specific commands supported by Redis.
 * 
 * @author Costin Leau
 */
public interface RedisSetCommands {

	Boolean sAdd(String key, String value);

	Boolean sRem(String key, String value);

	String sPop(String key);

	Boolean sMove(String srcKey, String destKey, String value);

	Integer sCard(String key);

	Boolean sIsMember(String key, String value);

	Set<String> sInter(String... keys);

	void sInterStore(String destKey, String... keys);

	Set<String> sUnion(String... keys);

	void sUnionStore(String destKey, String... keys);

	Set<String> sDiff(String... keys);

	void sDiffStore(String destKey, String... keys);

	Set<String> sMembers(String key);

	String sRandMember(String key);
}
