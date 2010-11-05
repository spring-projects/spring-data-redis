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
		//        return redisTemplate.getSetOperations().size(redisKey);
		throw new UnsupportedOperationException();
	}

	public boolean contains(Object o) {
		//TODO investigate cast
		//        return redisTemplate.getSetOperations().contains(redisKey, (String)o);
		throw new UnsupportedOperationException();
	}

	public Iterator iterator() {
		//        return redisTemplate.getSetOperations().getAll(redisKey).iterator();
		throw new UnsupportedOperationException();
	}

	public boolean add(Object o) {
		//TODO investigate cast
		//        return redisTemplate.getSetOperations().add(redisKey, (String)o);
		throw new UnsupportedOperationException();
	}

	public boolean remove(Object o) {
		//TODO investigate cast
		//        return redisTemplate.getSetOperations().remove(redisKey, (String)o);
		throw new UnsupportedOperationException();
	}


	public Set<String> members() {
		//        return redisTemplate.getSetOperations().getAll(redisKey);
		throw new UnsupportedOperationException();
	}

	/*
	public List<String> members(final int offset, final int max) {
	    return redisTemplate.sort(redisKey, redisTemplate.sortParams().limit(offset, max));

	}*/

	public String getRandom() {
		//        return redisTemplate.getSetOperations().getRandom(redisKey);
		throw new UnsupportedOperationException();
	}

	public boolean removeRandom() {
		//        return redisTemplate.getSetOperations().removeRandom(redisKey);
		throw new UnsupportedOperationException();
	}

	/*
	public intersection(RedisSet... redisSets) {
		//storeIntersectionOfSets..
		return null;
	}
	*/

	public RedisSet intersection(String newKey, RedisSet... redisSets) {
		throw new UnsupportedOperationException();
		/*
		String[] keys = new String[redisSets.length];
		int i = 0;
		for (RedisSet redisSet : redisSets) {
			keys[i] = redisSet.getRedisKey();
			i++;
		}
		redisTemplate.getSetOperations().storeIntersectionOfSets(newKey, keys);

		RedisSet resultSet = new RedisSet(redisTemplate, newKey);
		Set<String> results = redisTemplate.getSetOperations().getAll(newKey);
		resultSet.addAll(results);
		return resultSet;
		*/
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
