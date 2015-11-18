package io.vertx.amqp;

import io.vertx.core.AsyncResult;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonDelivery;
import io.vertx.proton.ProtonMessageHandler;
import io.vertx.proton.ProtonReceiver;

import java.util.concurrent.atomic.AtomicBoolean;

public class AMQPMessageConsumer<T> implements ProtonMessageHandler {

	private static final Logger log = LoggerFactory.getLogger(AMQPMessageConsumer.class);

	private final Vertx vertx;
	private final AMQPEventBus eventBus;
	private final String address;
	private AsyncResult<Void> result;
	private ProtonReceiver protonReceiver;
	private AtomicBoolean started = new AtomicBoolean(false);
	private final VertxConsumer<T> consumer;

	public AMQPMessageConsumer(Vertx vertx, AMQPEventBus eventBus, String address,VertxConsumer<T> consumer) {
		this.vertx = vertx;
		this.eventBus = eventBus;
		this.address = address;
		this.consumer = consumer;
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
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void handle(ProtonDelivery delivery, org.apache.qpid.proton.message.Message msg) {
		Object payload = eventBus.getMsgTranslator().toVertx(msg);
		AMQPVertxMsg amqpMsg = new AMQPVertxMsg(msg, payload);
		consumer.getHandler().handle(amqpMsg);
	}
}