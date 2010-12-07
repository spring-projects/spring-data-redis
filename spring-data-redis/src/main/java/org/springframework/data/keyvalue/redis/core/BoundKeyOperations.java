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
package org.springframework.data.keyvalue.redis.core;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.springframework.data.keyvalue.redis.connection.DataType;

/**
 * Key operations bound to a certain value.
 * 
 * @author Costin Leau
 */
public interface BoundKeyOperations<K> extends KeyBound<K> {

	Boolean exists();

	void delete();

	DataType type();

	void rename(K newKey);

	Boolean renameIfAbsent(K newKey);

	Boolean expire(long timeout, TimeUnit unit);

	Boolean expireAt(Date date);

	Long getExpire();

	void persist();
}
