package org.springframework.data.redis.mq.processor;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.MutablePropertyValues;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConstructorArgumentValues;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.Topic;
import org.springframework.data.redis.mq.adapter.RedisMessageListenerAdapter;
import org.springframework.data.redis.mq.annotation.RedisListener;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.util.ObjectUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author eric
 * @date 2019-08-12 14:37
 */
public class RedisBeanPostProcessor implements BeanPostProcessor, BeanFactoryAware {

    private BeanFactory beanFactory;

    private final AtomicInteger counter = new AtomicInteger();

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        Class<?> targetClass = AopUtils.getTargetClass(bean);
        ReflectionUtils.doWithMethods(targetClass, new ReflectionUtils.MethodCallback() {
			@Override
			public void doWith(Method method) throws IllegalArgumentException, IllegalAccessException {
				RedisListener redisListener = AnnotationUtils.findAnnotation(method, RedisListener.class);
				if (redisListener != null) {
					processRedisListener(redisListener, method, bean);
				}
			}
		});
        return bean;
    }

    private void processRedisListener(RedisListener redisListener, Method method, Object bean) {
        String containerFactory = redisListener.containerFactory();
        String[] topics = redisListener.topics();
        String id = redisListener.id();
        RedisMessageListenerContainer listenerContainer;
        if (!StringUtils.isEmpty(containerFactory)) {
            listenerContainer = beanFactory.getBean(containerFactory, RedisMessageListenerContainer.class);
            if (listenerContainer == null) {
                listenerContainer = beanFactory.getBean(RedisMessageListenerContainer.class);
            }
        } else {
            listenerContainer = beanFactory.getBean(RedisMessageListenerContainer.class);
        }
        if (StringUtils.isEmpty(id)) {
            id = "reids-sub-adapter#" + counter.getAndIncrement();
        }
        if (ObjectUtils.isEmpty(topics)) {
            throw new RuntimeException("topics must be not null");
        }
        ConstructorArgumentValues constructorArgumentValues = new ConstructorArgumentValues();
        constructorArgumentValues.addIndexedArgumentValue(0,bean);
        constructorArgumentValues.addIndexedArgumentValue(1,method.getName());
        MutablePropertyValues mutablePropertyValues = new MutablePropertyValues();
        mutablePropertyValues.addPropertyValue("id",id);
        RootBeanDefinition rootBeanDefinition = new RootBeanDefinition(RedisMessageListenerAdapter.class,constructorArgumentValues,mutablePropertyValues);
        DefaultListableBeanFactory factory=(DefaultListableBeanFactory)beanFactory;
        factory.registerBeanDefinition(id,rootBeanDefinition);
        RedisMessageListenerAdapter messageListenerAdapter = factory.getBean(id, RedisMessageListenerAdapter.class);
        messageListenerAdapter.setSerializer(serializer());
        if (topics.length>1) {
            Collection<Topic> collection = new ArrayList<>();
            HashSet<String> strings = new HashSet<>(Arrays.asList(topics));
            strings.forEach(s -> {
                collection.add(new PatternTopic(s));
            });
            listenerContainer.addMessageListener(messageListenerAdapter, collection);
        } else {
            listenerContainer.addMessageListener(messageListenerAdapter, new PatternTopic(topics[0]));
        }

    }

    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        this.beanFactory = beanFactory;
    }

    public static Jackson2JsonRedisSerializer<Object> serializer() {
        Jackson2JsonRedisSerializer<Object> serializer = new Jackson2JsonRedisSerializer<Object>(
                Object.class);
        ObjectMapper om = new ObjectMapper();
        om.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY);
        om.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
        serializer.setObjectMapper(om);
        return serializer;
    }

}
