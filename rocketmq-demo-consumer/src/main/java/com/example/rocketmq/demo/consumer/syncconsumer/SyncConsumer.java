package com.example.rocketmq.demo.consumer.syncconsumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @author SongNuoHui
 * @date 2022/1/26 10:40
 */
public class SyncConsumer {
  public static void main(String[] args) throws InterruptedException, MQClientException {

    // 实例化消费者
    DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("sync_consumer_unique_name");

    // 设置NameServer的地址
    consumer.setNamesrvAddr("localhost:9876");

    // 订阅一个或者多个Topic，以及Tag来过滤需要消费的消息
    consumer.subscribe("TopicTestSync", "*");
    // 注册回调实现类来处理从broker拉取回来的消息
    consumer.registerMessageListener(
        new MessageListenerConcurrently() {
          @Override
          public ConsumeConcurrentlyStatus consumeMessage(
              List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
            for (MessageExt msg : msgs) {
              //
                byte[] body = msg.getBody();
                String s = new String(body);
                System.out.println("消息体内容为：" + s);
            }

            System.out.printf(
                "%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
            // 标记该消息已经被成功消费
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
          }
        });
    // 启动消费者实例
    consumer.start();
    System.out.printf("Consumer Started.%n");
  }
}
