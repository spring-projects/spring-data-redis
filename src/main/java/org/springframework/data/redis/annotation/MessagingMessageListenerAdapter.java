package org.springframework.data.redis.annotation;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;

import org.jspecify.annotations.NonNull;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.util.ReflectionUtils;

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
