package com.qyf.mq.demo;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

public class Consumer {

//    public static void main(String[] args) throws InterruptedException, MQClientException {
//
//        // Instantiate with specified consumer group name.
//        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("please_rename_unique_group_name");
//
//        // Specify name server addresses.
//        consumer.setNamesrvAddr("localhost:9876");
//
//        // Subscribe one more more topics to consume.
//        consumer.subscribe("TopicTest", "*");
//        // Register callback to execute on arrival of messages fetched from brokers.
//        consumer.registerMessageListener(new MessageListenerConcurrently() {
//
//            @Override
//            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
//                                                            ConsumeConcurrentlyContext context) {
////                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
//                System.out.println(msgs);
//                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
//            }
//        });
//
//        //Launch the consumer instance.
//        consumer.start();
//
//        System.out.printf("Consumer Started.%n");
//    }

    public static void main(String[] args) throws Exception{
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("test_tansation_comsumer_name");
        consumer.setConsumeThreadMin(10);
        consumer.setConsumeThreadMax(10);
        //端口号一般是使用9876，Const.NAMESRV_ADDR=106.13.88.XXX:19876
        consumer.setNamesrvAddr("127.0.0.1:9876");
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
