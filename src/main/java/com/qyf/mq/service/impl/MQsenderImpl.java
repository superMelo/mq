package com.qyf.mq.service.impl;

import com.alibaba.fastjson.JSON;
import com.qyf.mq.entity.Content;
import com.qyf.mq.service.MQsender;
import lombok.extern.slf4j.Slf4j;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import java.util.UUID;


@Slf4j
@Service
public class MQsenderImpl implements MQsender {


    @Autowired
    private ApplicationContext applicationContext;

    @Override
    public void sendResult(Content text, String topic) {
        DefaultMQProducer producer = applicationContext.getBean(DefaultMQProducer.class);
        try {
            producer.start();
            //Create a message instance, specifying topic, tag and message body.
            Message msg = new Message(topic /* Topic */,
                    "TagA" /* Tag */,
                    (JSON.toJSONString(producer)).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
            );
            //Call send message to deliver message to one of brokers.
            SendResult sendResult = producer.send(msg);
            System.out.printf("%s%n", sendResult);

        } catch (Exception e) {
            log.error("{}", e.getMessage(), e);
        } finally {
            producer.shutdown();
        }
    }

    @Override
    public void sendResult(Content message, String topic, String tag) {
    }
}
