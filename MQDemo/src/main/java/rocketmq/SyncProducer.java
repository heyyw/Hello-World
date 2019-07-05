package rocketmq;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * @Desc: 描述
 * @Author: Heyyw
 * @CreateDate: 2019/7/5 14:37
 * @UpdateAuthor:
 * @UpdateDate:
 * @UpdateRemark: 更新说明
 * @Version: 1.0
 */
public class SyncProducer {
    public static void main(String[] args) throws Exception {
        //instantiate with a producer group name
        DefaultMQProducer producer = new DefaultMQProducer("heyyw");
        producer.setNamesrvAddr("47.102.210.38:9876");
        producer.setVipChannelEnabled(false);
        //lanch the instance
        producer.start();
        for (int i = 0; i < 100; i++) {
            //create a message instance, specifying topic,tag and message body
            Message msg = new Message("TopicTest","TagA",
                    ("Hello-RocketMq" + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            //call send message to deliver message to one of broker
            SendResult res = producer.send(msg);
            System.out.printf("%s%n",res);
        }
        //shutdown once the producer instance is not longer in use
        producer.shutdown();
    }
}
