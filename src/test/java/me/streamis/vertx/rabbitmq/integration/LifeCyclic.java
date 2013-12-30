package me.streamis.vertx.rabbitmq.integration;

import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.testtools.TestVerticle;
import org.vertx.testtools.VertxAssert;

/**
 * User: stream
 * Date: 13-12-30
 * Time: 12:07
 */
public class LifeCyclic extends TestVerticle {

  private static final String AMQP_BRIDGE_ADDR = "test.amqpBridge";

  private void appReady() {
    super.start();
  }


  public void start() {
    final JsonObject conf = new JsonObject();
    conf.putString("uri", "amqp://172.16.24.128:5672")
        .putString("address", AMQP_BRIDGE_ADDR)
        .putString("defaultContentType", "application/json");

    container.deployVerticle("me.streamis.vertx.rabbitmq.RabbitMQMod", conf, new AsyncResultHandler<String>() {
      @Override
      public void handle(AsyncResult<String> event) {
        if (event.failed()) {
          VertxAssert.fail(event.cause().getMessage());
          event.cause().printStackTrace();
          VertxAssert.testComplete();
        }
        appReady();
      }
    });
  }

  public void stop() {
    super.stop();
  }


  @Test
  public void testSend() {
    final EventBus eb = getVertx().eventBus();
    JsonObject json = new JsonObject();
    json.putObject("body", new JsonObject().
        putString("field1", "filed1").
        putString("field2", "field2"))
        .putString("routingKey", "*.*")
        .putString("exchange", "amq.direct");

    eb.send(AMQP_BRIDGE_ADDR + ".send", json, new Handler<Message<JsonObject>>() {
      @Override
      public void handle(Message<JsonObject> event) {
        VertxAssert.assertEquals(event.body().getString("status"), "ok");
        VertxAssert.testComplete();
      }
    });
  }

  @Test
  public void testConsume() {
    final EventBus eb = getVertx().eventBus();
    final Logger logger = getContainer().logger();
    final String handlerId = "testAddress1";

    eb.registerHandler(handlerId, new Handler<Message<JsonObject>>() {
      @Override
      public void handle(final Message<JsonObject> msg) {
        logger.info("received msg: " + msg.body());
        logger.info("reply address: " + msg.replyAddress());

        VertxAssert.assertEquals("amq.direct", msg.body().getString("exchange"));
        String contentType = msg.body().getObject("properties").getString("contentType");
        Object body;

        if ("application/json".equals(contentType) || "application/bson".equals(contentType)) {
          body = msg.body().getObject("body");
        } else {
          body = msg.body().getBinary("body");
        }
        logger.info("received body class: " + body.getClass());
        logger.info("received body: " + body);
        VertxAssert.assertTrue(body != null);
        //ack消息
        msg.reply(new JsonObject().putString("status", "ok"));
        VertxAssert.testComplete();
      }
    });

    logger.info("address for registered handler: " + handlerId);

    //注册消费者
    JsonObject createMsg = new JsonObject();
    createMsg.putString("exchange", "amq.direct");
    createMsg.putString("queueName", "vertx-rabbitmq");
    createMsg.putString("routingKey", "*.*");//"*.*"
    createMsg.putString("forward", handlerId);
    eb.send(AMQP_BRIDGE_ADDR + ".create-consumer", createMsg, new Handler<Message<JsonObject>>() {
      @Override
      public void handle(Message<JsonObject> event) {
        VertxAssert.assertEquals(event.body().getString("status"), "ok");
        logger.info("send message return ok");
      }
    });
  }
}









