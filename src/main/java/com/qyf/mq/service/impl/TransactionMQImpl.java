package com.qyf.mq.service.impl;

import com.qyf.mq.service.TransactionMQ;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;


@Service
public class TransactionMQImpl implements TransactionMQ {

    @Autowired
    private ApplicationContext applicationContext;


    @Override
    public void send() {
        TransactionMQProducer producer = applicationContext.getBean(TransactionMQProducer.class);
        try {
            Message message = new Message("tansaction_producer_topic","TagA","key", ("hello world!").getBytes("UTF-8"));
            producer.sendMessageInTransaction(message,"回调参数");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
