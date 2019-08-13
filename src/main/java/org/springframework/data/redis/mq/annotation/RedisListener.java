package org.springframework.data.redis.mq.annotation;

import java.lang.annotation.*;

/**
 * @author eric
 * @date 2019-08-12 14:28
 */
@Target({ ElementType.TYPE, ElementType.METHOD, ElementType.ANNOTATION_TYPE })
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RedisListener {

    /**
     * @return listener id
     */
    String id() default "";

    /**
     * @return listeners container
     */
    String containerFactory() default "redisMqMessageListenerContainer";

    /**
     * @return message topics
     */
    String[] topics() default {};

}
