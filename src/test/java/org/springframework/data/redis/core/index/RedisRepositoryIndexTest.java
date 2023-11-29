package org.springframework.data.redis.core.index;

import static org.junit.Assert.*;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.repository.configuration.EnableRedisRepositories;
import org.springframework.data.redis.test.util.RelaxedJUnit4ClassRunner;
import org.springframework.test.context.ContextConfiguration;

import kotlin.random.Random;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author Yan Ma
 */
@RunWith(RelaxedJUnit4ClassRunner.class)
@ContextConfiguration
public class RedisRepositoryIndexTest {

	final Log logger = LogFactory.getLog(getClass());

	@Configuration
	@EnableRedisRepositories(indexConfiguration = TestIndexConfiguration.class)
	public static class Config {

		@Bean
		RedisConnectionFactory connectionFactory() {

			JedisConnectionFactory connectionFactory = new JedisConnectionFactory();
			connectionFactory.setHostName(SettingsUtils.getHost());
			connectionFactory.setPort(SettingsUtils.getPort());

			JedisPoolConfig poolConfig = new JedisPoolConfig();
			poolConfig.setMaxWaitMillis(2000L);
			connectionFactory.setPoolConfig(poolConfig);
			connectionFactory.afterPropertiesSet();

			return connectionFactory;
		}

		@Bean
		RedisTemplate<?, ?> redisTemplate(RedisConnectionFactory connectionFactory) {
			RedisTemplate<byte[], byte[]> template = new RedisTemplate<>();
			template.setConnectionFactory(connectionFactory);
			return template;
		}
	}

	@Autowired TestPersonRepository repo;
	Jedis jedis = null;
	@Before
	public void setup(){
		repo.deleteAll();
	}

	@After
	public void cleanup(){
		repo.deleteAll();
		if(jedis!=null){
			jedis.close();
		}
	}

	@Test
	public void test_index_creation_and_simple_queries() {

		int random = Random.Default.nextInt(10);
		Date start = new Date();
		while (random == 0)
			random = Random.Default.nextInt(10);
		logger.info("random number = " + random);
		String lastName = "my.last.name" + new Date();
		String city = "Pleasanton";
		TestState testState = TestState.CA;
		for (int i = 0; i < random; i++) {
			TestPerson p = new TestPerson();
			p.id = UUID.randomUUID();
			String firstName = "my.first,name_" + p.id;
			p.firstName = firstName;
			p.lastName = lastName;
			p.address = new TestAddress();
			p.address.city = city;
			p.address.state = testState;
			p.address.streetNumber = 6220;
			p.address.street = "stoneridge mall rd";
			p.gender = TestGender.FEMALE;
			p.age = 25;
			repo.save(p);
			logger.info("after save: " + p);
			// test single match query by primary key
			Optional<TestPerson> findOne = repo.findById(p.id);
			assertNotNull(findOne);
			// test single match query by specify key value
			List<TestPerson> findByFirstName = repo.findByFirstName(firstName);
			assertEquals(1, findByFirstName.size());
		}
		Date end = new Date();
		// test range query
		List<TestPerson> findByCreatedTimestampBetween = repo.findByCreatedTimestampBetween(start, end);
		assertEquals(random, findByCreatedTimestampBetween.size());
		long oneMinuteAgo = start.getTime() - 60000;
		long oneSecondAgo = start.getTime() - 1000;
		List<TestPerson> findByCreatedTimestampBetween2 = repo.findByCreatedTimestampBetween(new Date(oneMinuteAgo), new Date(oneSecondAgo));
		assertEquals(0, findByCreatedTimestampBetween2.size());
		long oneSecondAfter = end.getTime() + 1000;
		long oneMinuteAfter = end.getTime() + 60000;
		List<TestPerson> findByCreatedTimestampBetween3 = repo.findByCreatedTimestampBetween(new Date(oneSecondAfter), new Date(oneMinuteAfter));
		assertEquals(0, findByCreatedTimestampBetween3.size());
		// test less than
		List<TestPerson> findByCreatedTimestampLessThan = repo.findByCreatedTimestampLessThan(end);
		assertEquals(random, findByCreatedTimestampLessThan.size());
		List<TestPerson> findByCreatedTimestampLessThan1 = repo.findByCreatedTimestampLessThan(start);
		assertEquals(0, findByCreatedTimestampLessThan1.size());
		// test less than equal
		List<TestPerson> findByCreatedTimestampLessThanEqual = repo.findByCreatedTimestampLessThanEqual(end);
		assertEquals(random, findByCreatedTimestampLessThanEqual.size());
		List<TestPerson> findByCreatedTimestampLessThanEqual1 = repo.findByCreatedTimestampLessThanEqual(start);
		assertTrue(findByCreatedTimestampLessThanEqual1.size() <= 1);
		// test batch simple query on simple attributes
		List<TestPerson> findByLastName = repo.findByLastName(lastName);
		assertEquals(random, findByLastName.size());
		List<TestPerson> findByLastName2 = repo.findByLastName("new_last_name"+new Date());
		assertEquals(0, findByLastName2.size());
		// test batch simple query on nested attributes
		List<TestPerson> findByAddressCity = repo.findByAddressCity(city);
		assertEquals(random, findByAddressCity.size());
		// To test the CompositeSortingIndex
		// The key set name should be West
		jedis = jedis==null? new Jedis() :jedis;
		String zset = "TestPerson" + ":" + TestAddress.getStateCategory(testState);
		assertEquals("zset", jedis.type(zset));
		// The total number of values should be the same as the random int
		assertTrue(random == jedis.zcard(zset));
		// They should be sorted per createdTimestamp
		Set<String> zrange = jedis.zrange(zset, 0, -1);
		double prev = 0;
		for(String key : zrange){
			double curr = Double.valueOf(jedis.hget("TestPerson" + ":" + key, "createdTimestamp"));
			assertTrue(prev < curr);
			prev = curr; 
		}
	}

	@Test
	public void test_complex_queries() throws InterruptedException {
		Date start = new Date();
		TestPerson p1 = new TestPerson();
		p1.id = UUID.randomUUID();
		String firstName1 = "f1";
		p1.firstName = firstName1;
		String lastName = "last";
		p1.lastName = lastName;
		p1.address = new TestAddress();
		String cityName = "city";
		p1.address.city = cityName;
		p1.address.state = TestState.CA;
		repo.save(p1);
		TestPerson p2 = new TestPerson();
		p2.id = UUID.randomUUID();
		p2.firstName = "f2";
		p2.lastName = lastName;
		p2.address = new TestAddress();
		p2.address.city = cityName;
		p2.address.state = TestState.CA;
		repo.save(p2);
		TestPerson p3 = new TestPerson();
		p3.id = UUID.randomUUID();
		p3.firstName = "f3";
		p3.lastName = lastName;
		p3.address = new TestAddress();
		p3.address.city = cityName;
		p3.address.state = TestState.CA;
		repo.save(p3);

		Date end = new Date();
		String SOME_POSTFIX = "something_else";
		// To test List<TestPerson> findByFirstNameAndLastName(String firstName, String lastName);
		List<TestPerson> findByFirstNameAndLastName = repo.findByFirstNameAndLastName(firstName1, lastName);
		assertEquals(1, findByFirstNameAndLastName.size());
		List<TestPerson> findByFirstNameAndLastName1 = repo.findByFirstNameAndLastName(firstName1+SOME_POSTFIX, lastName);
		assertEquals(0, findByFirstNameAndLastName1.size());
		List<TestPerson> findByFirstNameAndLastName2 = repo.findByFirstNameAndLastName(firstName1, lastName+SOME_POSTFIX);
		assertEquals(0, findByFirstNameAndLastName2.size());
		// To test List<TestPerson> findByFirstNameOrLastName(String firstName, String lastName);
		List<TestPerson> findByFirstNameOrLastName = repo.findByFirstNameOrLastName(firstName1, lastName);
		assertEquals(3, findByFirstNameOrLastName.size());
		List<TestPerson> findByFirstNameOrLastName1 = repo.findByFirstNameOrLastName(firstName1+SOME_POSTFIX, lastName);
		assertEquals(3, findByFirstNameOrLastName1.size());
		List<TestPerson> findByFirstNameOrLastName2 = repo.findByFirstNameOrLastName(firstName1, lastName+SOME_POSTFIX);
		assertEquals(1, findByFirstNameOrLastName2.size());
		// To test List<TestPerson> findByAddressCityAndCreatedTimestampBetween(String city, Date start, Date end);
		List<TestPerson> findByAddressCityAndCreatedTimestampBetween = repo.findByAddressCityAndCreatedTimestampBetween(cityName, start, end);
		assertEquals(3, findByAddressCityAndCreatedTimestampBetween.size());
		List<TestPerson> findByAddressCityAndCreatedTimestampBetween1 = repo.findByAddressCityAndCreatedTimestampBetween(cityName+SOME_POSTFIX, start, end);
		assertEquals(0, findByAddressCityAndCreatedTimestampBetween1.size());
		Date oneMinuteAgo = new Date(start.getTime() - 60000);
		Date oneSecondAgo = new Date(start.getTime() - 1000);
		List<TestPerson> findByAddressCityAndCreatedTimestampBetween2 = repo.findByAddressCityAndCreatedTimestampBetween(cityName, oneMinuteAgo, oneSecondAgo);
		assertEquals(0, findByAddressCityAndCreatedTimestampBetween2.size());
		Date oneSecondAfter = new Date(end.getTime() + 1000);
		Date oneMinuteAfter = new Date(end.getTime() + 60000);
		List<TestPerson> findByAddressCityAndCreatedTimestampBetween3 = repo.findByAddressCityAndCreatedTimestampBetween(cityName, oneSecondAfter, oneMinuteAfter);
		assertEquals(0, findByAddressCityAndCreatedTimestampBetween3.size());
		List<TestPerson> findByAddressCityAndCreatedTimestampBetween4 = repo.findByAddressCityAndCreatedTimestampBetween(cityName, oneMinuteAgo, oneMinuteAfter);
		assertEquals(3, findByAddressCityAndCreatedTimestampBetween4.size());
		// To test List<TestPerson> findByAddressCityOrCreatedTimestampGreaterThan(String city, Date start);
		List<TestPerson> findByAddressCityOrCreatedTimestampGreaterThan = repo.findByAddressCityOrCreatedTimestampGreaterThan(cityName, start);
		assertEquals(3, findByAddressCityOrCreatedTimestampGreaterThan.size());
		List<TestPerson> findByAddressCityOrCreatedTimestampGreaterThan1 = repo.findByAddressCityOrCreatedTimestampGreaterThan(cityName+SOME_POSTFIX, start);
		assertEquals(2, findByAddressCityOrCreatedTimestampGreaterThan1.size());
		List<TestPerson> findByAddressCityOrCreatedTimestampGreaterThan11 = repo.findByAddressCityOrCreatedTimestampGreaterThanEqual(cityName+SOME_POSTFIX, start);
		assertEquals(3, findByAddressCityOrCreatedTimestampGreaterThan11.size());
		List<TestPerson> findByAddressCityOrCreatedTimestampGreaterThan2 = repo.findByAddressCityOrCreatedTimestampGreaterThan(cityName, oneSecondAgo);
		assertEquals(3, findByAddressCityOrCreatedTimestampGreaterThan2.size());
		List<TestPerson> findByAddressCityOrCreatedTimestampGreaterThan3 = repo.findByAddressCityOrCreatedTimestampGreaterThan(cityName, oneSecondAfter);
		assertEquals(3, findByAddressCityOrCreatedTimestampGreaterThan3.size());
		List<TestPerson> findByAddressCityOrCreatedTimestampGreaterThan4 = repo.findByAddressCityOrCreatedTimestampGreaterThan(cityName+SOME_POSTFIX, oneSecondAgo);
		assertEquals(3, findByAddressCityOrCreatedTimestampGreaterThan4.size());
		List<TestPerson> findByAddressCityOrCreatedTimestampGreaterThan5 = repo.findByAddressCityOrCreatedTimestampGreaterThan(cityName+SOME_POSTFIX, oneSecondAfter);
		assertEquals(0, findByAddressCityOrCreatedTimestampGreaterThan5.size());
	}
	
}
