package io.vertx.amqp;

import io.vertx.core.AsyncResult;
import io.vertx.core.Closeable;
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
import io.vertx.proton.ProtonServer;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

public class AMQPEventBus implements EventBus, MetricsProvider {

	private static final Logger log = LoggerFactory.getLogger(AMQPEventBus.class);

	protected final VertxInternal vertx;
	protected final EventBusMetrics metrics;
	protected final ConcurrentMap<String, Handlers> handlerMap = new ConcurrentHashMap<>();
	private ProtonClient client;
	private ProtonServer server;
	private MessageTranslator msgTranslator;
	private volatile ProtonConnection protonConnection;
	protected volatile boolean started;

	// temp
	int inboundPort = 5673;
	String inboundHost = "localhost";
	int outboundPort = 5672;
	String outboundHost = "localhost";

	public AMQPEventBus(VertxInternal vertx) {
		this.vertx = vertx;
		this.metrics = vertx.metricsSPI().createMetrics(this);
		this.server = ProtonServer.create(vertx);
		this.server.connectHandler(this::processConnection);
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

		final CountDownLatch connectionReady = new CountDownLatch(1);
		server.listen(5672, res -> {
			if (res.failed()) {
				log.fatal("Inbound AMQP server failed to start");
				completionHandler.handle(Future.failedFuture(res.cause()));
				connectionReady.countDown();
			} else {
				log.info("Inbound AMQP server started and ready accept inbound connections");
			}
		});

		client.connect(outboundHost, outboundPort, res -> {
			if (res.succeeded()) {
				protonConnection = res.result();
				protonConnection.open();
				started = true;
				connectionReady.countDown();
				completionHandler.handle(Future.succeededFuture());
			} else {
				connectionReady.countDown();
				completionHandler.handle(Future.failedFuture(res.cause()));
			}
		});
		// Block start until the connection is ready. Any other operation
		// doesn't make sense until then.
		try {
			connectionReady.await();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
		}
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

	@Override
	public <T> MessageConsumer<T> consumer(String address) {
		checkStarted();
		Objects.requireNonNull(address, "address");
		return new AMQPMessageConsumer<T>(vertx, metrics, this, address, protonConnection, false, null, -1);
	}

	@Override
	public <T> MessageConsumer<T> consumer(String address, Handler<Message<T>> handler) {
		Objects.requireNonNull(handler, "handler");
		MessageConsumer<T> consumer = consumer(address);
		consumer.handler(handler);
		return consumer;
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
	public EventBus publish(String arg0, Object arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public EventBus publish(String arg0, Object arg1, DeliveryOptions arg2) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> MessageProducer<T> publisher(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> MessageProducer<T> publisher(String arg0, DeliveryOptions arg1) {
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
	public EventBus send(String arg0, Object arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> EventBus send(String arg0, Object arg1, Handler<AsyncResult<Message<T>>> arg2) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public EventBus send(String arg0, Object arg1, DeliveryOptions arg2) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> EventBus send(String arg0, Object arg1, DeliveryOptions arg2, Handler<AsyncResult<Message<T>>> arg3) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> MessageProducer<T> sender(String arg0) {
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
			throw new IllegalStateException("Event Bus is not started");
		}
	}

	private void processConnection(ProtonConnection connection) {

	}

	public <T> void addRegistration(String address, AMQPMessageConsumer<T> registration, boolean replyHandler,
	        boolean localOnly) {
		Objects.requireNonNull(registration.getHandler(), "handler");
		doAddRegistration(address, registration, replyHandler, localOnly);
		registration.setResult(Future.succeededFuture());
	}

	private void unregisterAll() {
	}

	public <T> void removeRegistration(String address, AMQPMessageConsumer<T> handler,
	        Handler<AsyncResult<Void>> completionHandler) {
		removeRegistration(address, handler);
		callCompletionHandlerAsync(completionHandler);
	}

	protected <T> HandlerHolder removeRegistration(String address, AMQPMessageConsumer<T> handler) {
		Handlers handlers = handlerMap.get(address);
		HandlerHolder lastHolder = null;
		if (handlers != null) {
			synchronized (handlers) {
				int size = handlers.list.size();
				// Requires a list traversal. This is tricky to optimise since
				// we can't use a set since
				// we need fast ordered traversal for the round robin
				for (int i = 0; i < size; i++) {
					HandlerHolder holder = handlers.list.get(i);
					if (holder.getHandler() == handler) {
						handlers.list.remove(i);
						holder.setRemoved();
						if (handlers.list.isEmpty()) {
							handlerMap.remove(address);
							lastHolder = holder;
						}
						holder.getContext().removeCloseHook(new HandlerEntry<>(address, holder.getHandler()));
						break;
					}
				}
			}
		}
		return lastHolder;
	}

	protected void callCompletionHandlerAsync(Handler<AsyncResult<Void>> completionHandler) {
		if (completionHandler != null) {
			vertx.runOnContext(v -> completionHandler.handle(Future.succeededFuture()));
		}
	}

	public class HandlerEntry<T> implements Closeable {
		final String address;
		final AMQPMessageConsumer<T> handler;

		public HandlerEntry(String address, AMQPMessageConsumer<T> handler) {
			this.address = address;
			this.handler = handler;
		}

		@Override
		public boolean equals(Object o) {
			if (o == null)
				return false;
			if (this == o)
				return true;
			if (getClass() != o.getClass())
				return false;
			HandlerEntry entry = (HandlerEntry) o;
			if (!address.equals(entry.address))
				return false;
			if (!handler.equals(entry.handler))
				return false;
			return true;
		}

		@Override
		public int hashCode() {
			int result = address != null ? address.hashCode() : 0;
			result = 31 * result + (handler != null ? handler.hashCode() : 0);
			return result;
		}

		// Called by context on undeploy
		public void close(Handler<AsyncResult<Void>> completionHandler) {
			handler.unregister(completionHandler);
			completionHandler.handle(Future.succeededFuture());
		}

	}
}
