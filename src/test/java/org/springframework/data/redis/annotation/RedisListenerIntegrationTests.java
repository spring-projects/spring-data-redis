/*
 * Copyright 2026 the original author or authors.
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
package org.springframework.data.redis.annotation;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.extension.JedisConnectionFactoryExtension;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.extension.LettuceConnectionFactoryExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.test.extension.RedisStandalone;

/**
 * Integration test for {@link EnableRedisListeners} and {@link RedisListener}.
 *
 * @author Mark Paluch
 * @author Ilyass Bougati
 */
@ParameterizedClass
@MethodSource("testParams")
public class RedisListenerIntegrationTests {

	private RedisConnectionFactory connectionFactory;
	private final AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();

	public RedisListenerIntegrationTests(RedisConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	static Collection<Arguments> testParams() {
		// Jedis
		JedisConnectionFactory jedisConnFactory = JedisConnectionFactoryExtension
				.getConnectionFactory(RedisStandalone.class);

		// Lettuce
		LettuceConnectionFactory lettuceConnFactory = LettuceConnectionFactoryExtension
				.getConnectionFactory(RedisStandalone.class);

		return List.of(Arguments.argumentSet("Jedis", jedisConnFactory),
				Arguments.argumentSet("Lettuce", lettuceConnFactory));
	}

	@Test // GH-1004
	void shouldListenForMessage() throws InterruptedException {

		context.registerBean("my-container", RedisMessageListenerContainer.class, () -> {

			RedisMessageListenerContainer container = new RedisMessageListenerContainer();
			container.setRecoveryInterval(100);
			container.setConnectionFactory(connectionFactory);
			return container;
		});

		context.register(Config.class);
		context.refresh();

		StringRedisTemplate template = new StringRedisTemplate();
		template.setConnectionFactory(connectionFactory);
		template.afterPropertiesSet();

		MyListener bean = context.getBean(MyListener.class);
		bean.message.clear();

		template.convertAndSend("my-channel-listener", "Hello Redis!");

		String message = bean.message.poll(10, TimeUnit.SECONDS);
		assertThat(message).isEqualTo("Hello Redis!");
	}

	@AfterEach
	void tearDown() {
		context.stop();
	}

	@Configuration
	@EnableRedisListeners
	static class Config {

		@Bean
		MyListener myListener() {
			return new MyListener();
		}
	}

	static class MyListener {

		LinkedBlockingQueue<String> message = new LinkedBlockingQueue<>();

		@RedisListener("my-channel-listener")
		void onMessage(String msg) {
			message.offer(msg);
		}

	}

}
