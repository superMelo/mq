package com.qyf.mq.listenter;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

@Slf4j
public class TransactionListenerImpl implements TransactionListener {

    //执行本地事务
    @Override
    public LocalTransactionState executeLocalTransaction(Message message, Object o) {
        log.info("-----执行事务-----");
//        log.info("message:{}", JSON.toJSONString(message));
//        log.info("obj:{}", JSON.toJSONString(o));
        System.out.println("-----执行事务-----");
        boolean state = false;
        if (!state) {
            return LocalTransactionState.UNKNOW;
        }
        return LocalTransactionState.COMMIT_MESSAGE;
    }


    //检查事务回滚 事务执行失败大约1分钟后会执行下面的方法
    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        log.info("---回滚----");
        String keys = msg.getKeys();
        log.info("key:{}", keys);
//        boolean state = false;
//        if (!state) {
//            for (int i = 0; i < 3; i++) {
//                return LocalTransactionState.ROLLBACK_MESSAGE;
//            }
//        }
        return LocalTransactionState.COMMIT_MESSAGE;
    }
}
