package org.springframework.data.redis.config;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Role;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.data.redis.annotation.RedisListenerAnnotationBeanPostProcessor;

@Configuration(proxyBeanMethods = false)
@Role(BeanDefinition.ROLE_INFRASTRUCTURE)
public class RedisListenerBootstrapConfiguration {

	@Bean
	@Role(BeanDefinition.ROLE_INFRASTRUCTURE)
	public RedisListenerEndpointRegistry redisListenerEndpointRegistry() {
		return new RedisListenerEndpointRegistry();
	}

	@Bean
	@Role(BeanDefinition.ROLE_INFRASTRUCTURE)
	public RedisListenerAnnotationBeanPostProcessor redisListenerAnnotationBeanPostProcessor(
			RedisListenerEndpointRegistry registry, BeanFactory beanFactory) {

		RedisListenerAnnotationBeanPostProcessor bpp = new RedisListenerAnnotationBeanPostProcessor();
		bpp.setEndpointRegistry(registry);
		bpp.setBeanFactory(beanFactory);
		return bpp;
	}
}
