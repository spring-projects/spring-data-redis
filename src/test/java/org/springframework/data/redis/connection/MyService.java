package org.springframework.data.redis.connection;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class MyService {

    private @Autowired
    RedisTemplate template;

    public MyService(RedisTemplate redisTemplate) {
        this.template = redisTemplate;
    }

    @Transactional
    public void valuesForKeys(List<String> keys) {
        for (String key : keys) {
            assert template.opsForValue().get(key) != null : "Could not fetch value for key!";
        }
    }
}
