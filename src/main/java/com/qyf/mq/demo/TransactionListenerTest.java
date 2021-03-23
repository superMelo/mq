package com.qyf.mq.demo;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

public class TransactionListenerTest implements TransactionListener{
    @Override
    public LocalTransactionState executeLocalTransaction(Message message, Object o) {

        System.out.println("执行事务");
        return LocalTransactionState.UNKNOW;
    }

    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
        System.out.println("检查事务");
        return LocalTransactionState.COMMIT_MESSAGE;
    }
}
