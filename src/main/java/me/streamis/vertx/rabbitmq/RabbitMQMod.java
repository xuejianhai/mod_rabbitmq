package me.streamis.vertx.rabbitmq;

import com.rabbitmq.client.*;
import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

/**
 * Prototype for AMQP bridge
 * Currently only does pub/sub and does not declare exchanges so only works with default exchanges
 * Three operations:
 * 1) Create a consumer on a topic given exchange name (use amqp.topic) and routing key (topic name)
 * 2) Close a consumer
 * 3) Send message
 *
 * @author <a href="http://tfox.org">Tim Fox</a>
 * @author <a href="http://github.com/blalor">Brian Lalor</a>
 * @author stream
 */
public class RabbitMQMod extends BusModBase {
  private Connection conn;
  private Map<Long, Channel> consumerChannels = new HashMap<>();
  private long consumerSeq;
  private Queue<Channel> availableChannels = new LinkedList<>();
  private ContentType defaultContentType;

  public void start() {
    super.start();
    final String address = getMandatoryStringConfig("address");
    String uri = getMandatoryStringConfig("uri");
    defaultContentType = ContentType.fromString(getMandatoryStringConfig("defaultContentType"));

    ConnectionFactory factory = new ConnectionFactory();

    try {
      factory.setUri(uri);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("illegal uri: " + uri, e);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalArgumentException("illegal uri: " + uri, e);
    } catch (KeyManagementException e) {
      throw new IllegalArgumentException("illegal uri: " + uri, e);
    }

    try {
      conn = factory.newConnection(); // IOException
    } catch (IOException e) {
      throw new IllegalStateException("Failed to create connection", e);
    }

    // register handlers
    eb.registerHandler(address + ".create-consumer", new Handler<Message<JsonObject>>() {
      public void handle(final Message<JsonObject> message) {
        handleCreateConsumer(message);
      }
    });

    eb.registerHandler(address + ".close-consumer", new Handler<Message<JsonObject>>() {
      public void handle(final Message<JsonObject> message) {
        handleCloseConsumer(message);
      }
    });

    eb.registerHandler(address + ".send", new Handler<Message<JsonObject>>() {
      public void handle(final Message<JsonObject> message) {
        handleSend(message);
      }
    });
  }


  public void stop() {
    consumerChannels.clear();
    if (conn != null) {
      try {
        conn.close();
      } catch (Exception e) {
        logger.warn("Failed to close", e);
      }
    }
  }

  /**
   * @return
   * @throws IOException
   */
  private Channel getChannel() throws IOException {
    if (!availableChannels.isEmpty()) {
      return availableChannels.remove();
    } else {
      return conn.createChannel(); // IOException
    }
  }

  private void handleSend(final Message<JsonObject> message) {
    try {
      send(null, message.body());
      sendOK(message);
    } catch (IOException e) {
      sendError(message, "unable to send: " + e.getMessage(), e);
    }
  }

  private void send(final AMQP.BasicProperties _props, final JsonObject message)
      throws IOException {
    AMQP.BasicProperties.Builder amqpPropsBuilder = new AMQP.BasicProperties.Builder();

    if (_props != null) {
      amqpPropsBuilder = _props.builder();
    }

    // correlationId and replyTo will already be set, if necessary

    JsonObject ebProps = message.getObject("properties");
    if (ebProps != null) {
      amqpPropsBuilder.clusterId(ebProps.getString("clusterId"));
      amqpPropsBuilder.contentType(ebProps.getString("contentType"));
      amqpPropsBuilder.contentEncoding(ebProps.getString("contentEncoding"));

      if (ebProps.getNumber("deliveryMode") != null) {
        amqpPropsBuilder.deliveryMode(ebProps.getNumber("deliveryMode").intValue());
      }

      amqpPropsBuilder.expiration(ebProps.getString("expiration"));

      if (ebProps.getObject("headers") != null) {
        amqpPropsBuilder.headers(ebProps.getObject("headers").toMap());
      }

      amqpPropsBuilder.messageId(ebProps.getString("messageId"));

      if (ebProps.getNumber("priority") != null) {
        amqpPropsBuilder.priority(ebProps.getNumber("priority").intValue());
      }

      // amqpPropsBuilder.timestamp(ebProps.getString("timestamp")); // @todo
      amqpPropsBuilder.type(ebProps.getString("type"));
      amqpPropsBuilder.userId(ebProps.getString("userId"));
    }

    Channel channel = getChannel();
    availableChannels.add(channel);

    ContentType contentType = defaultContentType;

    try {
      contentType = ContentType.fromString(amqpPropsBuilder.build().getContentType());
    } catch (IllegalArgumentException e) {
      logger.warn(
          "Illegal content type; using default " + defaultContentType.getContentType()
      );
      amqpPropsBuilder.contentType(contentType.getContentType());
    }

    byte[] messageBodyBytes;

    if (ContentType.JSON_CONTENT_TYPES.contains(contentType) ||
        (contentType == ContentType.TEXT_PLAIN)) {
      String contentEncoding = amqpPropsBuilder.build().getContentEncoding();
      if (contentEncoding == null) {
        contentEncoding = "UTF-8";
        amqpPropsBuilder.contentEncoding(contentEncoding);
      }

      try {
        if (contentType == ContentType.TEXT_PLAIN) {
          messageBodyBytes = message.getString("body").getBytes(contentEncoding);
        } else {
          messageBodyBytes = message.getObject("body").encode().getBytes(contentEncoding);
        }
      } catch (UnsupportedEncodingException e) {
        throw new IllegalStateException("unsupported encoding " + contentEncoding, e);
      }
    } else {
      throw new IllegalStateException("don't know how to transform " + contentType.getContentType());
    }

    channel.basicPublish(
        // exchange must default to non-null string
        message.getString("exchange", ""),
        message.getString("routingKey"), //queueName
        amqpPropsBuilder.build(),
        messageBodyBytes
    );
  }


  private long createConsumer(final String exchangeName, String queueName,
                              final String routingKey,
                              final String forwardAddress) throws IOException {

    final Channel channel = getChannel();
    Consumer cons = new MessageTransformingConsumer(channel, defaultContentType, logger) {

      public void doHandle(final String consumerTag,
                           final Envelope envelope,
                           final AMQP.BasicProperties properties,
                           final JsonObject body) throws IOException {
        final long deliveryTag = envelope.getDeliveryTag();

        eb.send(forwardAddress, body, new Handler<Message<JsonObject>>() {
          @Override
          public void handle(Message<JsonObject> event) {
            if (event.body().getString("status").equals("ok")) {
              try {
                getChannel().basicAck(deliveryTag, false);
              } catch (IOException e) {
                //TODO: 获取channel时发生IO异常
                container.logger().warn(e.getMessage(), e);
              }
            }
          }
        });
      }
    };

    if (queueName == null) {
      queueName = channel.queueDeclare().getQueue();
    }
    channel.queueBind(queueName, exchangeName, routingKey);

    //false is meaning not ack automatically
    channel.basicConsume(queueName, false, cons);
    long id = consumerSeq++;
    consumerChannels.put(id, channel);
    return id;
  }

  private void handleCreateConsumer(final Message<JsonObject> message) {
    String exchange = message.body().getString("exchange");
    String queueName = message.body().getString("queueName");
    String routingKey = message.body().getString("routingKey");
    String forwardAddress = message.body().getString("forward");

    JsonObject reply = new JsonObject();

    try {
      reply.putNumber("id", createConsumer(exchange, queueName, routingKey, forwardAddress));
      sendOK(message, reply);
    } catch (IOException e) {
      sendError(message, "unable to create consumer: " + e.getMessage(), e);
    }
  }

  private void handleCloseConsumer(final Message<JsonObject> message) {
    long id = (Long) message.body().getNumber("id");
    Channel channel = consumerChannels.remove(id);
    if (channel != null) {
      availableChannels.add(channel);
    }
  }


}
