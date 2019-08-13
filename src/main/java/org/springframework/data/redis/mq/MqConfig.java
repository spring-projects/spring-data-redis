package org.springframework.data.redis.mq;

import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.mq.processor.RedisBeanPostProcessor;

/**
 * @author eric
 * @date 2019-08-13 09:49
 */
public class MqConfig {

    @Bean
    public RedisBeanPostProcessor redisBeanPostProcessor() {
        return new RedisBeanPostProcessor();
    }

    @Bean(name = "redisMqMessageListenerContainer")
    RedisMessageListenerContainer container(RedisConnectionFactory connectionFactory) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        return container;
    }
}
