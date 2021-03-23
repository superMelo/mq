package com.qyf.mq.demo;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.concurrent.*;

public class Producer {

//    public static void main(String[] args) throws Exception {
//        //Instantiate with a producer group name.
//        DefaultMQProducer producer = new
//                DefaultMQProducer("please_rename_unique_group_name");
//        // Specify name server addresses.
//        producer.setNamesrvAddr("localhost:9876");
//        //Launch the instance.
//        producer.start();
//        for (int i = 0; i < 100; i++) {
//            //Create a message instance, specifying topic, tag and message body.
//            Message msg = new Message("TopicTest" /* Topic */,
//                    "TagA" /* Tag */,
//                    ("Hello RocketMQ " +
//                            i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
//            );
//            //Call send message to deliver message to one of brokers.
//            SendResult sendResult = producer.send(msg);
//            System.out.printf("%s%n", sendResult);
//        }
//        //Shut down once the producer instance is not longer in use.
//        producer.shutdown();
//    }

    public static void main(String[] args) throws Exception{
        TransactionMQProducer producer = new TransactionMQProducer("test_tansation_comsumer_name");
        ExecutorService executorService = new ThreadPoolExecutor(1, 5, 5, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(2000), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thead = new Thread(r);
                thead.setName("tansaction_producer_group"+"callback-thread");
                return thead;
            }
        });
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.setExecutorService(executorService);
        producer.setTransactionListener(new TransactionListenerTest());
        producer.start();
        Message message = new Message("tansaction_producer_topic","TagA","key", ("hello world!").getBytes("UTF-8"));
        TransactionSendResult sendResult = producer.sendMessageInTransaction(message, "回调参数");
        System.out.println(sendResult);
//        producer.shutdown();
    }
}
