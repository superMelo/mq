package com.qyf.mq.controller;

import com.qyf.mq.entity.Content;
import com.qyf.mq.service.MQsender;
import com.qyf.mq.service.TransactionMQ;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;


@Slf4j
@RestController
public class ProducerController {


    @Autowired
    private MQsender mqsender;

    @Autowired
    private TransactionMQ transactionMQ;

    @RequestMapping("send")
    public void send(String content, String topic) {
        Content message = new Content();
        message.setContent(content);
        message.setId(UUID.randomUUID().toString());
        message.setState("success");
        mqsender.sendResult(message, topic);
    }

    @RequestMapping("transactionSend")
    public void transactionSend(String content, String topic) {
        Content message = new Content();
        message.setContent(content);
        message.setId(UUID.randomUUID().toString());
        message.setState("success");
        transactionMQ.send();
    }
}
