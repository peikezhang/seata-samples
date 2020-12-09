package demo1;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.Scanner;
/*
发送同步消息
 */
public class Producer {
    public static void main(String[] args) throws Exception {
        /*
        group 相同的生产者成为一个生产者组

        标识发送同一类消息的Producer，通常发送逻辑一致。
        发送普通消息的时候，仅标识使用，并无特别用处。

        若发送事务消息，发送某条消息的producer-A宕机，
        使得事务消息一直处于PREPARED状态并超时，
        则broker会回查同一个group的其他producer，
        确认这条消息应该commit还是rollback。

        但开源版本并不完全支持事务消息（阉割了事务回查的代码）。?????
         */
        DefaultMQProducer p = new DefaultMQProducer("producer-demo1");

        /*
        连接nameserver集群, 获得注册的broker信息
         */
        p.setNamesrvAddr("192.168.64.151:9876:192.168.64.152:9876");
        p.start();

        /*
        主题相当于是消息的分类, 一类消息使用一个主题
         */
        String topic = "Topic1";

        /*
        tag 相当于是消息的二级分类, 在一个主题下, 可以通过 tag 再对消息进行分类
         */
        String tag = "TagA";

        while (true) {
            System.out.print("输入消息,用逗号分隔多条消息: ");
            String[] a = new Scanner(System.in).nextLine().split(",");

            for (String s : a) {
                Message msg = new Message(topic, tag, s.getBytes()); //一级分类, 二级分类, 消息内容
                SendResult r = p.send(msg);// 发送消息后会得到服务器反馈, 包含: smsgId, sendStatus, queue, queueOffset, offsetMsgId
                System.out.println(r);
            }
        }
    }
}
