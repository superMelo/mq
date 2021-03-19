package com.qyf.mq.config;


import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import javax.annotation.PostConstruct;
import java.util.List;

@Slf4j
@Configuration
public class MQConfig {

    @Value("${rocketmq.name-server}")
    private String nameServer;

    @Value("${rocketmq.timeout}")
    private int timeout;

    @Value("${spring.application.name}")
    private String group;

    @Value("${rocketmq.topic}")
    private String topic;

    @Scope("prototype")
    @Bean(name = "producer")
    public DefaultMQProducer defaultMQProducer() {
        DefaultMQProducer producer = new DefaultMQProducer(group);
        // Specify name server addresses.
        producer.setNamesrvAddr(nameServer);
        //消费超时时间
        producer.setSendMsgTimeout(timeout);
        return producer;
    }

    @PostConstruct
    public void defaultMQPushConsumer() {
        // Instantiate with specified consumer group name.
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group);

        // Specify name server addresses.
        consumer.setNamesrvAddr(nameServer);

        // Subscribe one more more topics to consume.
        try {
            consumer.subscribe(topic, "*");
            // Register callback to execute on arrival of messages fetched from brokers.
            consumer.registerMessageListener((List<MessageExt> msgs, ConsumeConcurrentlyContext context)->{
                try {
                    System.out.println(msgs);
                    //消费成功返回信息
                    throw new RuntimeException("异常出现");
//                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }catch (Exception e){
                    for (MessageExt msg : msgs) {
                        if (msg.getReconsumeTimes() == 3){
                            //先返回成功，将失败信息记录,后面再处理
                            log.error("达到重试限制" + msg.getReconsumeTimes());
                            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                        }
                    }
                    //消费失败返回信息
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            });
            //Launch the consumer instance.
            consumer.start();
            System.out.printf("Consumer Started.%n");
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }


}
