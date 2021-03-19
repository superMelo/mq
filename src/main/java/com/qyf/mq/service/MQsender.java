package com.qyf.mq.service;

import com.qyf.mq.entity.Content;

public interface MQsender {

    void sendResult(Content text, String topic);

    void sendResult(Content message, String topic, String tag);


}
