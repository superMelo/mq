package com.qyf.mq.config;


import com.qyf.mq.listenter.TransactionListenerImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.concurrent.*;

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
//                    throw new RuntimeException("异常出现");
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
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


    @Scope("prototype")
    @Bean(name = "transactionProducer")
    public TransactionMQProducer transactionMQProducer(){
        TransactionMQProducer producer = new TransactionMQProducer("test_tansation_comsumer_name");
        ExecutorService executorService = new ThreadPoolExecutor(2, 5, 5, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(2000), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thead = new Thread(r);
                thead.setName("tansaction_producer_group"+"callback-thread");
                return thead;
            }
        });
        producer.setNamesrvAddr(nameServer);
        producer.setExecutorService(executorService);
        //这个接口有两个方法，一个是异步执行本地事务，一个是回查
        TransactionListenerImpl transactionListener =new TransactionListenerImpl();
        producer.setTransactionListener(transactionListener);
        return producer;
    }


    @PostConstruct
    public void transactionMQPushConsumer() throws Exception{
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("test_tansation_comsumer_name");
        consumer.setConsumeThreadMin(10);
        consumer.setConsumeThreadMax(10);
        //端口号一般是使用9876，Const.NAMESRV_ADDR=106.13.88.XXX:19876
        consumer.setNamesrvAddr(nameServer);
        //从那个位置开始消费，可以从末尾，最前端  这里是最末尾
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        //消费哪个主题的消息和标签，标签可以是表达式，如*表示消费该topic下的所有类型标签的消息
        consumer.subscribe("tansaction_producer_topic","*");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                MessageExt me = msgs.get(0);
                try{
                    String topic = me.getTopic();
                    String tags = me.getTags();
                    String keys =me.getKeys();
//                    if(keys.equals("key3")){
//                        System.out.println("模拟消费失败*********");
//                        int a = 1/0;
//                    }
                    String body = new String(me.getBody(), "UTF-8");
                    System.out.println("test_transation消费消息：topic:"+topic+"tags:"+tags+"keys:"+keys+"body:"+body);
                }catch (Exception e){
                    e.printStackTrace();
                    int reconsumerTimes = me.getReconsumeTimes();
                    System.out.println("第"+reconsumerTimes+"次消费该消息!");
                    if(reconsumerTimes==4){
                        //如果消费四次还没有消费到就做日志，然后做补偿
                    }
                    //消费失败会重新消费，时间从1s 开始然后越来越长，到最后一次重试的时间是2h 默认重试15次
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
    }


}
