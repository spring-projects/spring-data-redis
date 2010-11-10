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
package org.springframework.datastore.redis.util;

import java.util.UUID;

import org.springframework.datastore.redis.connection.RedisCommands;
import org.springframework.datastore.redis.connection.jedis.JedisConnectionFactory;


/**
 * String-based Redis List test.
 * 
 * @author Costin Leau
 */
public class StringRedisListTest extends AbstractRedisCollectionTest<String> {

	private DefaultRedisList<String> redisList;

	public StringRedisListTest() {
		JedisConnectionFactory factory = new JedisConnectionFactory();
		factory.afterPropertiesSet();
		String redisName = getClass().getName();
		RedisCommands commands = factory.getConnection();
		redisList = new DefaultRedisList<String>(redisName, commands);
		

		//		SimpleRedisSerializer serializer = new SimpleRedisSerializer();
		//		
		//		String t = getT();
		//		
		//		String data = serializer.serializeAsString(t);
		//		String name = "some-list";
		//		System.out.println(data);
		//		commands.lPush(name, data);
		//		List<String> readData = commands.lRange(name, 0, -1);
		//		System.out.println(readData);
		//		System.out.println(serializer.deserialize(readData.get(0)));

	}

	@Override
	AbstractRedisCollection<String> getCollection() {
		return redisList;
	}

	@Override
	String getT() {
		return UUID.randomUUID().toString();
	}
}

