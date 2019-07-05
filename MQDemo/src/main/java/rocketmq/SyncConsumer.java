package rocketmq;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @Desc: 描述
 * @Author: Heyyw
 * @CreateDate: 2019/7/5 14:52
 * @UpdateAuthor:
 * @UpdateDate:
 * @UpdateRemark: 更新说明
 * @Version: 1.0
 */
public class SyncConsumer {
    public static void main(String[] args) throws Exception{
        //instatiate with specified consumer group name
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("heyywconsume");

        //specify name server address
        consumer.setNamesrvAddr("47.102.210.38:9876");
        consumer.setVipChannelEnabled(false);
        //sepcify where to start in case the specified consumer group is as brand new one
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        //subscribe one more topics to consume
        consumer.subscribe("TopicTest","*");

        //register callback to execute on arrival of messages fetched from brokers
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                System.out.printf(Thread.currentThread().getName() + " receive msg: " + list + "%n");
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        //launch the consumer instance
        consumer.start();
    }
}
