package com.qyf.mq.delay;


import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Set;


@Service
@Slf4j
public class RedisClient {

    @Autowired
    private RedisTemplate redisTemplate;

    public void add(String key, String obj, Long l){
        redisTemplate.opsForZSet().add(key, obj, l);
    }

    public Set range(String key, Integer start, Integer end){
        Set range = redisTemplate.opsForZSet().range(key, start, end);
        return range;
    }

    public Long del(String key, Object obj){
        Long count = redisTemplate.opsForZSet().remove(key, obj);
        return count;
    }

}
