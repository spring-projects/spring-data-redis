/*
 * Copyright 2026-present the original author or authors.
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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.context.annotation.Import;
import org.springframework.data.redis.config.RedisListenerBootstrapConfiguration;
import org.springframework.data.redis.config.RedisListenerEndpointRegistry;

/**
 * Enable Redis Pub/Sub listener annotated endpoints that are created under the cover by a
 * {@link RedisListenerAnnotationBeanPostProcessor}. To be used on
 * {@link org.springframework.context.annotation.Configuration @Configuration} classes as follows:
 *
 * <pre class="code">
 * &#064;Configuration
 * &#064;EnableRedisListeners
 * public class AppConfig {
 *
 * 	&#064;Bean
 * 	public RedisMessageListenerContainer myRedisListenerContainer() {
 * 		RedisMessageListenerContainer factory = new RedisMessageListenerContainer();
 * 		factory.setConnectionFactory(connectionFactory());
 * 		return factory;
 * 	}
 *
 * 	// other &#064;Bean definitions
 * }
 * </pre>
 * <p>
 * The {@code RedisListenerAnnotationBeanPostProcessor} is responsible for creating endpoints.
 * <p>
 * {@code @EnableRedisListeners} enables detection of {@link RedisListener @RedisListener} annotations on any
 * Spring-managed bean in the container. For example, given a class {@code MyService}:
 *
 * <pre class="code">
 * package com.acme.foo;
 *
 * public class MyService {
 *
 * 	&#064;RedisListener(container = "myRedisListenerContainer", topic = "myChannel")
 * 	public void process(String msg) {
 * 		// process incoming message
 * 	}
 * }
 * </pre>
 * <p>
 * The container to use is identified by the {@link RedisListener#container() container} attribute defining the name of
 * the {@code RedisMessageListenerContainer} bean to use. When none is set a default
 * {@code RedisMessageListenerContainer} bean is assumed to be present.
 * <p>
 * The following configuration would ensure that every time a Pub/Sub is received on the topic channel named
 * "myChannel", {@code MyService.process()} is invoked with the content of the message:
 *
 * <pre class="code">
 * &#064;Configuration
 * &#064;EnableRedisListeners
 * public class AppConfig {
 *
 * 	&#064;Bean
 * 	public MyService myService() {
 * 		return new MyService();
 * 	}
 *
 * 	// Redis infrastructure setup
 * }
 * </pre>
 * <p>
 * Alternatively, if {@code MyService} were annotated with {@code @Component}, the following configuration would ensure
 * that its {@code @EnableRedisListeners} annotated method is invoked with a matching incoming message:
 *
 * <pre class="code">
 * &#064;Configuration
 * &#064;EnableRedisListeners
 * &#064;ComponentScan(basePackages = "com.acme.foo")
 * public class AppConfig {}
 * </pre>
 * <p>
 * Annotated methods can use flexible signature; in particular, it is possible to use the
 * {@link org.springframework.messaging.Message Message} abstraction and related annotations, see {@link RedisListener}
 * Javadoc for more details. For instance, the following would inject the content of the message and the "channel" name
 * header:
 *
 * <pre class="code">
 * &#064;RedisListener(container = "myRedisListenerContainer", topic = "myChannel")
 * public void process(String msg, @Header("channel") String channel) {
 * 	// process incoming message
 * }
 * </pre>
 * <p>
 * These features are abstracted by the
 * {@link org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory} that is responsible for
 * building the necessary invoker to process the annotated method. By default,
 * {@link org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory} is used.
 * <p>
 *
 * @author Ilyass Bougati
 * @since 4.1
 * @see RedisListener
 * @see RedisListenerAnnotationBeanPostProcessor
 * @see RedisListenerEndpointRegistry
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(RedisListenerBootstrapConfiguration.class)
public @interface EnableRedisListeners {

}
