/*
 * Copyright 2013 the original author or authors.
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
package org.springframework.data.redis.core;

import java.util.Arrays;
import java.util.Collection;

import org.springframework.data.redis.DoubleObjectFactory;
import org.springframework.data.redis.LongObjectFactory;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.StringObjectFactory;
import org.springframework.data.redis.connection.srp.SrpConnectionFactory;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * Parameters for testing implementations of {@link AbstractOperations}
 *
 * @author Jennifer Hickey
 *
 */
abstract public class AbstractOperationsTestParams {

	public static Collection<Object[]> testParams() {
		ObjectFactory<String> stringFactory = new StringObjectFactory();
		ObjectFactory<Long> longFactory = new LongObjectFactory();
		ObjectFactory<Double> doubleFactory = new DoubleObjectFactory();
		SrpConnectionFactory srConnFactory = new SrpConnectionFactory();
		srConnFactory.setPort(SettingsUtils.getPort());
		srConnFactory.setHostName(SettingsUtils.getHost());
		srConnFactory.afterPropertiesSet();
		RedisTemplate<String,String> stringTemplate = new StringRedisTemplate();
		stringTemplate.setConnectionFactory(srConnFactory);
		stringTemplate.afterPropertiesSet();
		RedisTemplate<String,Long> longTemplate = new RedisTemplate<String,Long>();
		longTemplate.setKeySerializer(new StringRedisSerializer());
		longTemplate.setValueSerializer(new GenericToStringSerializer<Long>(Long.class));
		longTemplate.setConnectionFactory(srConnFactory);
		longTemplate.afterPropertiesSet();
		RedisTemplate<String,Double> doubleTemplate = new RedisTemplate<String,Double>();
		doubleTemplate.setKeySerializer(new StringRedisSerializer());
		doubleTemplate.setValueSerializer(new GenericToStringSerializer<Double>(Double.class));
		doubleTemplate.setConnectionFactory(srConnFactory);
		doubleTemplate.afterPropertiesSet();
		return Arrays.asList(new Object[][] { { stringTemplate, stringFactory, stringFactory },
			{ longTemplate, stringFactory, longFactory }, { doubleTemplate, stringFactory, doubleFactory }
		});
	}
}
