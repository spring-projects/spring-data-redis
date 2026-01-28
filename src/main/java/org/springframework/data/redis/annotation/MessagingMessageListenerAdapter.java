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
import java.nio.charset.StandardCharsets;

import org.jspecify.annotations.NonNull;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.util.ReflectionUtils;

/**
 * An adapter that delegates {@link MessageListener#onMessage} to a target method via reflection.
 *
 * <p>Supports argument resolution for:
 * <ul>
 * <li>{@link Message} (The raw Redis message)</li>
 * <li>{@code byte[]} (The message body)</li>
 * <li>{@link String} (The message body decoded as UTF-8)</li>
 * </ul>
 *
 * @author Ilyass Bougati
 */
public class MessagingMessageListenerAdapter implements MessageListener {

	private Object bean;
	private Method method;

	public void setHandlerMethod(Object bean, Method method) {
		this.bean = bean;
		this.method = method;
		ReflectionUtils.makeAccessible(this.method);
	}

	@Override
	public void onMessage(@NonNull Message message, byte[] pattern) {
		try {
			Object[] arguments = resolveArguments(message, pattern);
			method.invoke(bean, arguments);
		} catch (Exception e) {
			// TODO: Integrate with @RedisExceptionHandler later
			throw new RuntimeException("Failed to invoke Redis listener method", e);
		}
	}

	private Object[] resolveArguments(Message message, byte[] pattern) {
		Class<?>[] parameterTypes = method.getParameterTypes();
		Object[] args = new Object[parameterTypes.length];

		for (int i = 0; i < parameterTypes.length; i++) {
			Class<?> type = parameterTypes[i];

			if (type.equals(Message.class)) {
				args[i] = message;
			} else if (type.equals(byte[].class)) {
				args[i] = message.getBody();
			} else if (type.equals(String.class)) {
				args[i] = new String(message.getBody(), StandardCharsets.UTF_8);
			} else {
				// TODO: Handle POJOs later
				args[i] = null;
			}
		}
		return args;
	}
}
