package org.springframework.datastore.redis.util;

import java.util.Collection;
import java.util.Set;

/**
 * 
 * @author Graeme Rocher
 *
 */
public interface RedisCollection extends Collection {

    /**
     * They key used by the collection
     *
     * @return The redis key
     */    
    String getRedisKey();
    
    Set<String> members();
}
