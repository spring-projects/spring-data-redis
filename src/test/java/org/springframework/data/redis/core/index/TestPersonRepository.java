package org.springframework.data.redis.core.index;

import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.springframework.data.repository.CrudRepository;

interface TestPersonRepository extends CrudRepository<TestPerson, UUID> {

	List<TestPerson> findByLastName(String lastName);
	List<TestPerson> findByAddressCity(String city);
	List<TestPerson> findByFirstName(String firstName);
	List<TestPerson> findByCreatedTimestampBetween(Date start, Date end);
	List<TestPerson> findByCreatedTimestampLessThan(Date end);
	List<TestPerson> findByCreatedTimestampLessThanEqual(Date end);
	List<TestPerson> findByFirstNameAndLastName(String firstName, String lastName);
	List<TestPerson> findByFirstNameOrLastName(String firstName, String lastName);
	List<TestPerson> findByAddressCityAndCreatedTimestampBetween(String city, Date start, Date end);
	List<TestPerson> findByAddressCityOrCreatedTimestampGreaterThan(String city, Date start);
	List<TestPerson> findByAddressCityOrCreatedTimestampGreaterThanEqual(String city, Date start);
}
