package io.vertx.amqp;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonDelivery;
import io.vertx.proton.ProtonMessageHandler;
import io.vertx.proton.ProtonReceiver;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

public class AMQPMessageConsumer<T> implements ProtonMessageHandler {

	private static final Logger log = LoggerFactory.getLogger(AMQPMessageConsumer.class);

	private final Vertx vertx;
	private final AMQPEventBus eventBus;
	private final String address;
	private List<VertxConsumer<T>> vertxConsumers = new ArrayList<VertxConsumer<T>>();
	private AsyncResult<Void> result;
	private ProtonReceiver protonReceiver;
	private AtomicBoolean started = new AtomicBoolean(false);
	private Random randomGenerator = new Random();

	public AMQPMessageConsumer(Vertx vertx, AMQPEventBus eventBus, String address) {
		this.vertx = vertx;
		this.eventBus = eventBus;
		this.address = address;
	}

	ProtonReceiver protocolReceiver() {
		return protonReceiver;
	}

	void setConnection(ProtonConnection connection) {
		if (!started.get()) {
			started.set(true);
			protonReceiver = connection.createReceiver(address).handler(this);
			protonReceiver.open();
			protonReceiver.closeHandler(this::handleClose);
			protonReceiver.flow(1000);
		}
	}

	private void handleClose(AsyncResult<ProtonReceiver> result) {

	}

	public void close() {
		if (protonReceiver != null) {
			protonReceiver.close();
		}
	}

	public void addHandler(VertxConsumer<T> handler) {
		vertxConsumers.add(handler);
	}

	public void removeHandler(VertxConsumer<T> handler) {
		vertxConsumers.remove(handler);
	}

	public int vertxConsumerCount(){
		return vertxConsumers.size();
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void handle(ProtonDelivery delivery, org.apache.qpid.proton.message.Message msg) {
		Object payload = eventBus.getMsgTranslator().toVertx(msg);
		AMQPVertxMsg amqpMsg = new AMQPVertxMsg(msg, payload);
		System.out.println("Address : " +  msg.getAddress());
		if (msg.getAddress().contains(eventBus.getPublishPrefix())) {
			for (VertxConsumer<T> consumer : vertxConsumers) {
				if (consumer.getHandler() != null){
					consumer.getHandler().handle(amqpMsg);
				}
			}
		} else {
			// Need to add round robin support
			Handler<Message<T>> handler = null;
			while (handler == null){
    			int index = vertxConsumers.size() == 1 ? 0 : randomGenerator.nextInt(vertxConsumers.size());
    			System.out.println("Notifying consumer : " + index);
    			handler = vertxConsumers.get(index).getHandler();
    			if (handler != null){
    			  handler.handle(amqpMsg);
    			} else {
    				// This shouldn't be the case .. but just in case.
    				if (vertxConsumers.size() == 1){
    					break;
    				}
    			}
			}
		}
	}
}