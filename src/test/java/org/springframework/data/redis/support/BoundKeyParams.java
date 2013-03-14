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
package org.springframework.data.redis.support;

import java.util.Arrays;
import java.util.Collection;

import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.jredis.JredisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.rjc.RjcConnectionFactory;
import org.springframework.data.redis.connection.srp.SrpConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.support.atomic.RedisAtomicInteger;
import org.springframework.data.redis.support.atomic.RedisAtomicLong;
import org.springframework.data.redis.support.collections.DefaultRedisList;
import org.springframework.data.redis.support.collections.DefaultRedisMap;
import org.springframework.data.redis.support.collections.DefaultRedisSet;
import org.springframework.data.redis.support.collections.RedisList;
import org.springframework.data.redis.support.collections.StringObjectFactory;

/**
 * @author Costin Leau
 * @author Jennifer Hickey
 */
public class BoundKeyParams {

	public static Collection<Object[]> testParams() {
		// Jedis
		JedisConnectionFactory jedisConnFactory = new JedisConnectionFactory();
		jedisConnFactory.setPort(SettingsUtils.getPort());
		jedisConnFactory.setHostName(SettingsUtils.getHost());
		jedisConnFactory.afterPropertiesSet();

		StringRedisTemplate templateJS = new StringRedisTemplate(jedisConnFactory);
		DefaultRedisMap mapJS = new DefaultRedisMap("bound:key:map", templateJS);
		DefaultRedisSet setJS = new DefaultRedisSet("bound:key:set", templateJS);
		RedisList list = new DefaultRedisList("bound:key:list", templateJS);

		// JRedis
		JredisConnectionFactory jredisConnFactory = new JredisConnectionFactory();
		jredisConnFactory.setPort(SettingsUtils.getPort());
		jredisConnFactory.setHostName(SettingsUtils.getHost());
		jredisConnFactory.afterPropertiesSet();

		StringRedisTemplate templateJR = new StringRedisTemplate(jredisConnFactory);
		DefaultRedisMap mapJR = new DefaultRedisMap("bound:key:mapJR", templateJR);
		// Skip list and set. Rename in Collections uses Redis tx, not supported by JRedis

		// Lettuce
		LettuceConnectionFactory lettuceConnFactory = new LettuceConnectionFactory();
		lettuceConnFactory.setPort(SettingsUtils.getPort());
		lettuceConnFactory.setHostName(SettingsUtils.getHost());
		lettuceConnFactory.afterPropertiesSet();

		StringRedisTemplate templateLT = new StringRedisTemplate(lettuceConnFactory);
		DefaultRedisMap mapLT = new DefaultRedisMap("bound:key:mapLT", templateLT);
		DefaultRedisSet setLT = new DefaultRedisSet("bound:key:setLT", templateLT);
		RedisList listLT = new DefaultRedisList("bound:key:listLT", templateLT);

		// SRP
		SrpConnectionFactory srpConnFactory = new SrpConnectionFactory();
		srpConnFactory.setPort(SettingsUtils.getPort());
		srpConnFactory.setHostName(SettingsUtils.getHost());
		srpConnFactory.afterPropertiesSet();

		StringRedisTemplate templateSRP = new StringRedisTemplate(srpConnFactory);
		DefaultRedisMap mapSRP = new DefaultRedisMap("bound:key:mapSRP", templateSRP);
		DefaultRedisSet setSRP = new DefaultRedisSet("bound:key:setSRP", templateSRP);
		RedisList listSRP = new DefaultRedisList("bound:key:listSRP", templateSRP);


		// RJC
		RjcConnectionFactory rjcConnFactory = new RjcConnectionFactory();
		rjcConnFactory.setPort(SettingsUtils.getPort());
		rjcConnFactory.setHostName(SettingsUtils.getHost());
		rjcConnFactory.afterPropertiesSet();

		StringRedisTemplate templateRJC = new StringRedisTemplate(rjcConnFactory);
		DefaultRedisMap mapRJC = new DefaultRedisMap("bound:key:mapRJC", templateRJC);
		DefaultRedisSet setRJC = new DefaultRedisSet("bound:key:setRJC", templateRJC);
		RedisList listRJC = new DefaultRedisList("bound:key:listRJC", templateRJC);

		StringObjectFactory sof = new StringObjectFactory();

		return Arrays.asList(new Object[][] {
				{ new RedisAtomicInteger("bound:key:int", jedisConnFactory), sof, templateJS },
				{ new RedisAtomicLong("bound:key:long", jedisConnFactory), sof, templateJS },
				{ list, sof, templateJS }, { setJS, sof, templateJS }, { mapJS, sof, templateJS },
				{ new RedisAtomicInteger("bound:key:intJR", jredisConnFactory), sof, templateJR },
				{ new RedisAtomicLong("bound:key:longJR", jredisConnFactory), sof, templateJR },
				{ mapJR, sof, templateJR },
				{ new RedisAtomicInteger("bound:key:intLT", lettuceConnFactory), sof, templateLT },
				{ new RedisAtomicLong("bound:key:longLT", lettuceConnFactory), sof, templateLT },
				{ listLT, sof, templateLT }, { setLT, sof, templateLT }, { mapLT, sof, templateLT },
				{ new RedisAtomicInteger("bound:key:intSrp", srpConnFactory), sof, templateSRP },
				{ new RedisAtomicLong("bound:key:longSrp", srpConnFactory), sof, templateSRP },
				{ listSRP, sof, templateSRP }, { setSRP, sof, templateSRP }, { mapSRP, sof, templateSRP },
				{ new RedisAtomicInteger("bound:key:intRjc", rjcConnFactory), sof, templateRJC },
				{ new RedisAtomicLong("bound:key:longRjc", rjcConnFactory), sof, templateRJC },
				{ listRJC, sof, templateRJC }, { setRJC, sof, templateRJC }, { mapRJC, sof, templateRJC }});
	}
}
