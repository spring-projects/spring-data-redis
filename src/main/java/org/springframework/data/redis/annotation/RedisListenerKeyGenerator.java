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

import java.lang.reflect.Method;

import org.jspecify.annotations.Nullable;

/**
 * Strategy to group {@link RedisListener @RedisListener} methods into a single listener.
 * <p>
 * Methods of the same bean, registered with the same container, whose generated keys are
 * {@link Object#equals(Object) equal} are merged into one listener subscribed to all their topics. Each message is
 * dispatched sequentially, in no particular order, to the methods listening to the topic it was received on.
 * Subscription callbacks are delivered once per topic. Methods with a {@literal null} key get a listener of their own.
 * <p>
 * For example, with {@link #perBeanAndTopic()}, three methods of one bean listening on {@code ch1} result in one
 * listener and one task per message. With {@link #perBean()}, methods of one bean listening on {@code ch1} and
 * {@code ch2} share a single listener. Of each grouped endpoint, only the listener it creates is used; see
 * {@link org.springframework.data.redis.config.BatchMethodRedisListenerEndpoint}.
 * <p>
 * Picked up from the application context if exactly one bean of this type is present.
 *
 * @author Moritz Halbritter
 * @since 4.2
 * @see RedisListenerAnnotationBeanPostProcessor#setKeyGenerator(RedisListenerKeyGenerator)
 */
@FunctionalInterface
public interface RedisListenerKeyGenerator {

	/**
	 * Generate the grouping key for a {@link RedisListener @RedisListener} annotation. Invoked once per annotation, so a
	 * method annotated for {@code ch1} and {@code ch2} is asked twice.
	 *
	 * @param bean the bean declaring the method
	 * @param method the annotated method
	 * @param listener the annotation, merged with attributes of composed annotations
	 * @param topic the resolved topic
	 * @return the grouping key, or {@literal null} to not group the annotation
	 */
	@Nullable
	Object generate(Object bean, Method method, RedisListener listener, String topic);

	/**
	 * Return a {@link RedisListenerKeyGenerator} grouping the methods of a bean listening to the same topic.
	 */
	static RedisListenerKeyGenerator perBeanAndTopic() {
		return (bean, method, listener, topic) -> topic;
	}

	/**
	 * Return a {@link RedisListenerKeyGenerator} grouping all methods of a bean.
	 */
	static RedisListenerKeyGenerator perBean() {

		// any constant works: grouping is already scoped to bean and container
		return (bean, method, listener, topic) -> RedisListenerKeyGenerator.class;
	}

}
