package org.springframework.datastore.redis.util;

import java.util.Iterator;
import java.util.Set;

import org.springframework.datastore.redis.core.RedisTemplate;

/**
 * 
 * @author Graeme Rocher
 *
 */
public class RedisSet extends AbstractRedisCollection implements Set {

    public RedisSet(RedisTemplate redisTemplate, String redisKey) {
        super(redisTemplate, redisKey);
    }

    public int size() {
        return redisTemplate.getSetOperations().size(redisKey);
    }

    public boolean contains(Object o) {
    	//TODO investigate cast
        return redisTemplate.getSetOperations().contains(redisKey, (String)o);
    }

    public Iterator iterator() {
        return redisTemplate.getSetOperations().getAll(redisKey).iterator();        
    }

    public boolean add(Object o) {
    	//TODO investigate cast
        return redisTemplate.getSetOperations().add(redisKey, (String)o);
    }

    public boolean remove(Object o) {
    	//TODO investigate cast
        return redisTemplate.getSetOperations().remove(redisKey, (String)o);
    }


    public Set<String> members() {
        return redisTemplate.getSetOperations().getAll(redisKey);  
    }

    /*
    public List<String> members(final int offset, final int max) {
        return redisTemplate.sort(redisKey, redisTemplate.sortParams().limit(offset, max));

    }*/

    public String getRandom() {
        return redisTemplate.getSetOperations().getRandom(redisKey);
    }

    public boolean removeRandom() {
        return redisTemplate.getSetOperations().removeRandom(redisKey);
    }
    
	
	void intersection(RedisSet... redisSets) {
		//storeIntersectionOfSets..
	}
	
	void union(RedisSet... redisSets) {
		//storeUnionOfSets
	}
	
	void difference(RedisSet... redisSets) {
		
	}	
	
	//consider methods in google collections such as 	
	// cartesianProduct, filter, powerSet, symmetricDifference, newRedisSet
    //TODO move to another set
    
    //
}
