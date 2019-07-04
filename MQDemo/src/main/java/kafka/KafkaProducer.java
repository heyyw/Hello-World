package kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;
import java.util.UUID;

/**
 * @Desc: 描述
 * @Author: Heyyw
 * @CreateDate: 2019/7/4 14:48
 * @UpdateAuthor:
 * @UpdateDate:
 * @UpdateRemark: 更新说明
 * @Version: 1.0
 */
public class KafkaProducer {
    private final Producer<String, String> producer;
        public final static String TOPIC = "TEST-TOPIC";

    private KafkaProducer(){
        Properties props = new Properties();
        //此处配置的是kafka的端口
        props.put("metadata.broker.list", "47.102.210.38:9092");

        //配置value的序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //配置key的序列化类
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");

        //request.required.acks
        //0, which means that the producer never waits for an acknowledgement from the broker (the same behavior as 0.7). This option provides the lowest latency but the weakest durability guarantees (some data will be lost when a server fails).
        //1, which means that the producer gets an acknowledgement after the leader replica has received the data. This option provides better durability as the client waits until the server acknowledges the request as successful (only messages that were written to the now-dead leader but not yet replicated will be lost).
        //-1, which means that the producer gets an acknowledgement after all in-sync replicas have received the data. This option provides the best durability, we guarantee that no messages will be lost as long as at least one in sync replica remains.
        props.put("request.required.acks","-1");

        producer = new Producer<String, String>(new ProducerConfig(props));
    }

    void produce() {
        int messageNo = 100;
        final int COUNT = 1000;

        while (messageNo < COUNT) {
            String key = String.valueOf(messageNo);
            String data = "hello kafka message " + key;
            producer.send(new KeyedMessage<String, String>(TOPIC, key ,data));
            System.out.println(data);
            messageNo ++;
        }
    }
    //http://www.open-open.com/lib/view/open1412991579999.html
    public static void main( String[] args )
    {
        new KafkaProducer().produce();
    }

//    public static void main(String[] args) {
//     /**
//                  * 1、指定当前kafka producer生产的数据的目的地
//                  *  创建topic可以输入以下命令，在kafka集群的任一节点进行创建。
//                  *  bin/kafka-topics.sh --create --zookeeper zk01:2181 --replication-factor 1 --partitions 1 --topic test
//                  */
//        String TOPIC = "orderMq";
//         /**
//           * 2、读取配置文件
//           */
//        Properties props = new Properties();
//         /*
//         * key.serializer.class默认为serializer.class
//         */
//         props.put("serializer.class", "kafka.serializer.StringEncoder");
//        /*
//         * kafka broker对应的主机，格式为host1:port1,host2:port2
//         */
//        props.put("metadata.broker.list", "47.102.210.38:9092");
//        /*
//         * request.required.acks,设置发送数据是否需要服务端的反馈,有三个值0,1,-1
//         * 0，意味着producer永远不会等待一个来自broker的ack，这就是0.7版本的行为。
//         * 这个选项提供了最低的延迟，但是持久化的保证是最弱的，当server挂掉的时候会丢失一些数据。
//         * 1，意味着在leader replica已经接收到数据后，producer会得到一个ack。
//         * 这个选项提供了更好的持久性，因为在server确认请求成功处理后，client才会返回。
//         * 如果刚写到leader上，还没来得及复制leader就挂了，那么消息才可能会丢失。
//         * -1，意味着在所有的ISR都接收到数据后，producer才得到一个ack。
//         * 这个选项提供了最好的持久性，只要还有一个replica存活，那么数据就不会丢失
//         */
//         props.put("request.required.acks", "1");
//        /*
//         * 可选配置，如果不配置，则使用默认的partitioner partitioner.class
//         * 默认值：kafka.producer.DefaultPartitioner
//         * 用来把消息分到各个partition中，默认行为是对key进行hash。
//         */
//        //props.put("partitioner.class", "cn.itcast.storm.kafka.MyLogPartitioner");
//        props.put("partitioner.class", "kafka.producer.DefaultPartitioner");
//        /**
//                  * 3、通过配置文件，创建生产者
//                  */
//        Producer<String, String> producer = new Producer<String, String>(new ProducerConfig(props));
//         /**
//                  * 4、通过for循环生产数据
//                  */
//        for (int messageNo = 1; messageNo < 100000; messageNo++)
//                /*"用来配合自定义的MyLogPartitioner进行数据分发"
//                      * 5、调用producer的send方法发送数据
//                      * 注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发
//                      */
//         producer.send(new KeyedMessage<String, String>(TOPIC, messageNo + "", "appid" + UUID.randomUUID() + "itcast"));
//    }
}
