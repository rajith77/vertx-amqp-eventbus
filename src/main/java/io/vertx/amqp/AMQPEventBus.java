package io.vertx.amqp;

import static io.vertx.proton.ProtonHelper.tag;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.spi.metrics.EventBusMetrics;
import io.vertx.core.spi.metrics.Metrics;
import io.vertx.core.spi.metrics.MetricsProvider;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonSender;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class AMQPEventBus implements EventBus, MetricsProvider {

	private static final Logger log = LoggerFactory.getLogger(AMQPEventBus.class);

	protected final VertxInternal vertx;
	protected final EventBusMetrics metrics;
	protected final ConcurrentMap<String, AMQPMessageConsumer> receiverMap = new ConcurrentHashMap<String, AMQPMessageConsumer>();
	protected final ConcurrentMap<String, ProtonSender> senderMap = new ConcurrentHashMap<String, ProtonSender>();
	private ProtonClient client;
	private MessageTranslator msgTranslator;
	private volatile ProtonConnection protonConnection;
	protected volatile boolean started;
	private AtomicInteger counter = new AtomicInteger();

	// temp
	int outboundPort = 5672;
	String outboundHost = "localhost";
	private String publishPrefix = System.getProperty("vertx-amqp-pub-prefix", "");
	private String sendPrefix = System.getProperty("vertx-amqp-send-prefix", "");
	
	public AMQPEventBus(VertxInternal vertx) {
		System.out.println("====================");
		System.out.println("   AMQP Event Bus   ");
		System.out.println("====================");
		this.vertx = vertx;
		client = ProtonClient.create(vertx);	
		this.metrics = vertx.metricsSPI().createMetrics(this);
		msgTranslator = MessageTranslator.Factory.create();
	}

	@Override
	public boolean isMetricsEnabled() {
		return metrics != null && metrics.isEnabled();
	}

	@Override
	public synchronized void start(Handler<AsyncResult<Void>> completionHandler) {
		if (started) {
			throw new IllegalStateException("Already started");
		}
		
		client.connect(outboundHost, outboundPort, res -> {
			if (res.succeeded()) {
				protonConnection = res.result();
				protonConnection.open();
				for (AMQPMessageConsumer cons : receiverMap.values()){
					if (cons.getHandler() != null) {
						cons.setConnection(protonConnection);
					}
				}
				started = true;
				completionHandler.handle(Future.succeededFuture());
			} else {
				completionHandler.handle(Future.failedFuture(res.cause()));
			}
		});
	}

	@Override
	public void close(Handler<AsyncResult<Void>> completionHandler) {
		checkStarted();
		unregisterAll();
		if (metrics != null) {
			metrics.close();
		}
		if (completionHandler != null) {
			vertx.runOnContext(v -> completionHandler.handle(Future.succeededFuture()));
		}
	}
	
	void addRegistration(String address, AMQPMessageConsumer cons){
		receiverMap.put(address, cons);
	}
	
	void removeRegistration(String address, AMQPMessageConsumer cons, Handler<AsyncResult<Void>> completionHandler){
		receiverMap.remove(address);
		//TODO completionHandler;
	}

	void unregisterAll(){
		for (ProtonSender sender : senderMap.values()){
			sender.close();
		}
		
		for (AMQPMessageConsumer reciever : receiverMap.values()){
			reciever.protocolReceiver().close();
		}
		
		senderMap.clear();
		receiverMap.clear();
		protonConnection.close();
	}
	
	@Override
	public <T> MessageConsumer<T> consumer(String address) {
		return createConsumer(address, null);
	}

	@Override
	public <T> MessageConsumer<T> consumer(String address, Handler<Message<T>> handler) {
		Objects.requireNonNull(handler, "handler");
		return createConsumer(address, handler);
	}
	
	<T> MessageConsumer<T> createConsumer(String address, Handler<Message<T>> handler){
		checkStarted();
		Objects.requireNonNull(address, "address");
		
		AMQPMessageConsumer<T> cons = new AMQPMessageConsumer<T>(vertx, metrics, this, address, protonConnection, false, null, -1);
		if (handler != null){
			cons.handler(handler);
			if (started){
				System.out.println("Connection is ready. Setting it");
				cons.setConnection(protonConnection);
			}
		}		
		return cons;
	}

	@Override
	public <T> MessageConsumer<T> localConsumer(String address) {
		return consumer(address);
	}

	@Override
	public <T> MessageConsumer<T> localConsumer(String address, Handler<Message<T>> handler) {
		return consumer(address, handler);
	}

	@Override
	public EventBus publish(String address, Object msg) {
		return publish(address, msg, new DeliveryOptions());
	}

	@Override
	public EventBus publish(String address, Object vertxMsg, DeliveryOptions options) {
		String amqpAddress = publishPrefix + address;
		org.apache.qpid.proton.message.Message msg = msgTranslator.toAMQP(vertxMsg);
		msg.setAddress(amqpAddress);
		ProtonSender sender = null;
		if (senderMap.containsKey(amqpAddress)){
			sender = senderMap.get(amqpAddress);
		}else{
			sender = protonConnection.createSender(amqpAddress);
			sender.open();
			senderMap.put(amqpAddress, sender);			
		}
		sender.send(tag(String.valueOf(counter)), msg);
		return this;
	}

	@Override
	public <T> MessageProducer<T> publisher(String address) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> MessageProducer<T> publisher(String address, DeliveryOptions arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public EventBus registerCodec(MessageCodec arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> EventBus registerDefaultCodec(Class<T> arg0, MessageCodec<T, ?> arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public EventBus send(String address, Object msg) {
		return send(address, msg, new DeliveryOptions());
	}

	@Override
	public <T> EventBus send(String address, Object msg, Handler<AsyncResult<Message<T>>> replyHandler) {
		return send(address, msg, new DeliveryOptions(), replyHandler);
	}

	@Override
	public EventBus send(String address, Object vertxMsg, DeliveryOptions options) {
		String amqpAddress = sendPrefix + address;
		org.apache.qpid.proton.message.Message msg = msgTranslator.toAMQP(vertxMsg);
		msg.setAddress(amqpAddress);
		if (senderMap.containsKey(amqpAddress)){
			senderMap.get(amqpAddress).send(tag(String.valueOf(counter)), msg);
		}else{
			ProtonSender sender = protonConnection.createSender(amqpAddress);
			sender.open();
			senderMap.put(amqpAddress, sender);
			sender.send(tag(String.valueOf(counter)), msg);
		}
		return this;
	}

	@Override
	public <T> EventBus send(String address, Object vertxMsg, DeliveryOptions options, Handler<AsyncResult<Message<T>>> replyHandler) {
		String amqpAddress = sendPrefix + address;
		org.apache.qpid.proton.message.Message msg = msgTranslator.toAMQP(vertxMsg);
		msg.setAddress(amqpAddress);
		ProtonSender sender = null;
		if (senderMap.containsKey(amqpAddress)){
			sender = senderMap.get(amqpAddress);
		}else{
		    sender = protonConnection.createSender(amqpAddress);
			senderMap.put(amqpAddress, sender);			
		}
		sender.send(tag(String.valueOf(counter)), msg, delivery -> {
			System.out.println("The message was received by the AMQP Peer, notifying the Verticle");
			AsyncResult<Message<T>> result = Future.succeededFuture();
			replyHandler.handle(result);
		});
		return this;
	}

	@Override
	public <T> MessageProducer<T> sender(String address) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> MessageProducer<T> sender(String arg0, DeliveryOptions arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public EventBus unregisterCodec(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public EventBus unregisterDefaultCodec(Class arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Metrics getMetrics() {
		return metrics;
	}

	protected void checkStarted() {
		if (!started) {
			//throw new IllegalStateException("Event Bus is not started");			
		}
	}
	
	MessageTranslator getMsgTranslator(){
		return msgTranslator;
	}
}
