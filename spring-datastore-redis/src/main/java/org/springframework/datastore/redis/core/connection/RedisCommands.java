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

package org.springframework.datastore.redis.core.connection;

import java.util.Collection;

/**
 * Commands supported by Redis .
 * 
 * @author Costin Leau
 */
public interface RedisCommands {

	boolean exists(String key);

	int del(String... keys);

	DataTypes type(String key);

	Collection<String> keys(String pattern);

	String randomKey();

	//TODO see whether the status code can be properly intercepted
	boolean rename(String oldName, String newName);

	boolean renameNx(String oldName, String newName);

	int dbSize();

	boolean expire(String key, long seconds);

	boolean persist(String key);

	int ttl(String key);

	void select(int dbIndex);

	void watch(String... keys);

	void unwatch();

	void multi();

	void exec();

	void discard();
}