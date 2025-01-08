/*
 * Copyright 2011-2025 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.support.collections;

import static org.assertj.core.api.Assertions.*;

import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.StringObjectFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.extension.JedisConnectionFactoryExtension;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.support.collections.RedisCollectionFactoryBean.CollectionType;
import org.springframework.data.redis.test.extension.RedisStanalone;

/**
 * Integration tests for {@link RedisCollectionFactoryBean}.
 *
 * @author Costin Leau
 * @author Mark Paluch
 * @author Christoph Strobl
 */
public class RedisCollectionFactoryBeanTests {

	protected ObjectFactory<String> factory = new StringObjectFactory();
	protected StringRedisTemplate template;
	protected RedisStore col;

	RedisCollectionFactoryBeanTests() {
		JedisConnectionFactory jedisConnFactory = JedisConnectionFactoryExtension
				.getConnectionFactory(RedisStanalone.class);

		this.template = new StringRedisTemplate(jedisConnFactory);
	}

	@BeforeEach
	void setUp() {

		this.template.delete("key");
		this.template.delete("nosrt");
	}

	@AfterEach
	void tearDown() throws Exception {

		// clean up the whole db
		template.execute((RedisCallback<Object>) connection -> {
			connection.serverCommands().flushDb();
			return null;
		});
	}

	private RedisStore createCollection(String key) {
		return createCollection(key, null);
	}

	private RedisStore createCollection(String key, CollectionType type) {

		RedisCollectionFactoryBean fb = new RedisCollectionFactoryBean();
		fb.setKey(key);
		fb.setTemplate(template);
		fb.setType(type);
		fb.afterPropertiesSet();

		return fb.getObject();
	}

	@Test
	void testNone() {

		RedisStore store = createCollection("nosrt", CollectionType.PROPERTIES);
		assertThat(store).isInstanceOf(RedisProperties.class);

		store = createCollection("nosrt", CollectionType.MAP);
		assertThat(store).isInstanceOf(DefaultRedisMap.class);

		store = createCollection("nosrt", CollectionType.SET);
		assertThat(store).isInstanceOf(DefaultRedisSet.class);

		store = createCollection("nosrt", CollectionType.LIST);
		assertThat(store).isInstanceOf(DefaultRedisList.class);

		store = createCollection("nosrt");
		assertThat(store).isInstanceOf(DefaultRedisList.class);
	}

	@Test // GH-2633
	void testExisting() {

		template.delete("key");
		template.opsForHash().put("key", "k", "v");

		assertThat(createCollection("key")).isInstanceOf(DefaultRedisMap.class);
		assertThat(createCollection("key", CollectionType.MAP)).isInstanceOf(DefaultRedisMap.class);

		template.delete("key");
		template.opsForSet().add("key", "1", "2");

		assertThat(createCollection("key")).isInstanceOf(DefaultRedisSet.class);
		assertThat(createCollection("key", CollectionType.SET)).isInstanceOf(DefaultRedisSet.class);

		template.delete("key");
		template.opsForList().leftPush("key", "1", "2");

		assertThat(createCollection("key")).isInstanceOf(DefaultRedisList.class);
		assertThat(createCollection("key", CollectionType.LIST)).isInstanceOf(DefaultRedisList.class);
	}

	@Test
	void testExistingCol() {

		String key = "set";
		String val = "value";

		template.boundSetOps(key).add(val);
		RedisStore col = createCollection(key);
		assertThat(col).isInstanceOf(DefaultRedisSet.class);

		key = "map";
		template.boundHashOps(key).put(val, val);
		col = createCollection(key);
		assertThat(col).isInstanceOf(DefaultRedisMap.class);

		col = createCollection(key, CollectionType.PROPERTIES);
		assertThat(col).isInstanceOf(RedisProperties.class);
	}

	@Test // GH-2633
	void testIncompatibleCollections() {

		template.opsForValue().set("key", "value");
		assertThatIllegalArgumentException().isThrownBy(() -> createCollection("key", CollectionType.LIST))
				.withMessageContaining("Cannot create collection type 'LIST' for a key containing 'STRING'");

		template.delete("key");
		template.opsForList().leftPush("key", "value");
		assertThatIllegalArgumentException().isThrownBy(() -> createCollection("key", CollectionType.SET))
				.withMessageContaining("Cannot create collection type 'SET' for a key containing 'LIST'");
	}

	@Test // GH-2633
	void shouldFailForStreamCreation() {

		template.opsForStream().add("key", Map.of("k", "v"));
		assertThatIllegalArgumentException().isThrownBy(() -> createCollection("key", CollectionType.LIST))
				.withMessageContaining("Cannot create store on keys of type 'STREAM'");
	}

	@Test // Gh-2633
	void shouldFailWhenNotInitialized() {

		RedisCollectionFactoryBean fb = new RedisCollectionFactoryBean();
		fb.setKey("key");
		fb.setTemplate(template);
		fb.setType(CollectionType.SET);

		assertThatExceptionOfType(IllegalStateException.class).isThrownBy(() -> fb.getObject());
	}

	@Test // Gh-2633
	void usesBeanNameIfNoKeyProvided() {

		template.delete("key");
		template.opsForHash().put("key", "k", "v");

		RedisCollectionFactoryBean fb = new RedisCollectionFactoryBean();
		fb.setBeanName("key");
		fb.setTemplate(template);
		fb.afterPropertiesSet();

		assertThat(fb.getObject()).satisfies(value -> {
			assertThat(value).isInstanceOf(RedisMap.class);
			assertThat((RedisMap)value).containsEntry("k", "v");
		});
	}
}
