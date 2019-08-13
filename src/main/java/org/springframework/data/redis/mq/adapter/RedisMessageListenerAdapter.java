package org.springframework.data.redis.mq.adapter;

import org.springframework.data.redis.connection.DefaultMessage;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;

/**
 * @author eric
 * @date 2019-08-12 15:25
 */
public class RedisMessageListenerAdapter extends MessageListenerAdapter {

	public RedisMessageListenerAdapter(Object delegate, String defaultListenerMethod) {
		super(delegate, defaultListenerMethod);
	}

	/**
	 * @param message the Redis <code>Message</code>
	 * @return An instance of type {@link Message}
	 */
	@Override
	protected Object extractMessage(Message message) {
		Object o = super.extractMessage(message);
		if (o instanceof Message) {
			return o;
		} else {
			return new DefaultMessage(null, o.toString().getBytes());
		}
	}
}
