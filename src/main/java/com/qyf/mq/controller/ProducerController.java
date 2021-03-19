package com.qyf.mq.controller;

import com.qyf.mq.entity.Content;
import com.qyf.mq.service.MQsender;
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


    @RequestMapping("send")
    public void send(String content, String topic) {
        Content message = new Content();
        message.setContent(content);
        message.setId(UUID.randomUUID().toString());
        message.setState("success");
        mqsender.sendResult(message, topic);
    }
}
