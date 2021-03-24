package com.qyf.mq;

import com.qyf.mq.delay.RedisClient;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Set;

@SpringBootTest
@Slf4j
class MqApplicationTests {

	@Test
	void contextLoads() {
	}


	@Autowired
	private RedisClient redisClient;


	private static final String key = "test";

	@Test
	void add(){
		redisClient.add(key, "1", 2L);
		redisClient.add(key, "2", 1L);
		Set range = redisClient.range(key, 0, 5);
		log.info("range:{}", range);

	}
}
