package io.vertx.amqp;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.eventbus.ReplyFailure;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.spi.metrics.EventBusMetrics;
import io.vertx.core.streams.ReadStream;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public class VertxConsumer<T> implements MessageConsumer<T> {

	private static final Logger log = LoggerFactory.getLogger(VertxConsumer.class);

	private final Vertx vertx;
	private final EventBusMetrics metrics;
	private final AMQPEventBus eventBus;
	private final String address;
	private final boolean replyHandler;
	private final boolean isLocal;
	private final Handler<AsyncResult<Message<T>>> asyncResultHandler;
	private long timeoutID;
	private boolean registered;
	private Handler<Message<T>> handler;
	private AsyncResult<Void> result;
	private Handler<AsyncResult<Void>> completionHandler;
	private Handler<Void> endHandler;
	private Handler<Message<T>> discardHandler;
	private int maxBufferedMessages;
	private boolean paused;
	private Object metric;
	private AtomicBoolean started = new AtomicBoolean(false);

	private final String ID;

	public VertxConsumer(Vertx vertx, EventBusMetrics metrics, AMQPEventBus eventBus, String address,
	        boolean replyHandler, Handler<AsyncResult<Message<T>>> asyncResultHandler, long timeout, boolean isLocal) {
		ID = UUID.randomUUID().toString();
		this.isLocal = isLocal;
		this.vertx = vertx;
		this.metrics = metrics;
		this.eventBus = eventBus;
		this.address = address;
		this.replyHandler = replyHandler;
		this.asyncResultHandler = asyncResultHandler;
		if (timeout != -1) {
			timeoutID = vertx.setTimer(timeout, tid -> {
				metrics.replyFailure(address, ReplyFailure.TIMEOUT);
				sendAsyncResultFailure(ReplyFailure.TIMEOUT, "Timed out waiting for a reply");
			});
		}

	}

	String getID() {
		return ID;
	}

	boolean isLocal(){
		return isLocal;
	}
	
	@Override
	public synchronized MessageConsumer<T> setMaxBufferedMessages(int maxBufferedMessages) {
		this.maxBufferedMessages = maxBufferedMessages;
		return this;
	}

	@Override
	public synchronized int getMaxBufferedMessages() {
		return maxBufferedMessages;
	}

	@Override
	public String address() {
		return address;
	}

	@Override
	public synchronized void completionHandler(Handler<AsyncResult<Void>> completionHandler) {
		Objects.requireNonNull(completionHandler);
		if (result != null) {
			AsyncResult<Void> value = result;
			vertx.runOnContext(v -> completionHandler.handle(value));
		} else {
			this.completionHandler = completionHandler;
		}
	}

	@Override
	public synchronized void unregister() {
		unregister(false);
	}

	@Override
	public synchronized void unregister(Handler<AsyncResult<Void>> completionHandler) {
		Objects.requireNonNull(completionHandler);
		doUnregister(completionHandler, false);
	}

	public void unregister(boolean callEndHandler) {
		doUnregister(null, callEndHandler);
	}

	public void sendAsyncResultFailure(ReplyFailure failure, String msg) {
		unregister();
		asyncResultHandler.handle(Future.failedFuture(new ReplyException(failure, msg)));
	}

	private void doUnregister(Handler<AsyncResult<Void>> completionHandler, boolean callEndHandler) {
		if (timeoutID != -1) {
			vertx.cancelTimer(timeoutID);
		}
		if (endHandler != null && callEndHandler) {
			Handler<Void> theEndHandler = endHandler;
			Handler<AsyncResult<Void>> handler = completionHandler;
			completionHandler = ar -> {
				theEndHandler.handle(null);
				if (handler != null) {
					handler.handle(ar);
				}
			};
		}
		if (registered) {
			registered = false;
			eventBus.removeRegistration(address, this, completionHandler);
		} else {
			callCompletionHandlerAsync(completionHandler);
		}
		registered = false;
	}

	private void callCompletionHandlerAsync(Handler<AsyncResult<Void>> completionHandler) {
		if (completionHandler != null) {
			vertx.runOnContext(v -> completionHandler.handle(Future.succeededFuture()));
		}
	}

	public synchronized void setResult(AsyncResult<Void> result) {
		this.result = result;
		if (completionHandler != null) {
			if (result.succeeded()) {
				metric = metrics.handlerRegistered(address, replyHandler);
			}
			Handler<AsyncResult<Void>> callback = completionHandler;
			vertx.runOnContext(v -> callback.handle(result));
		} else if (result.failed()) {
			log.error("Failed to propagate registration for handler " + handler + " and address " + address);
		} else {
			metric = metrics.handlerRegistered(address, replyHandler);
		}
	}

	/*
	 * Internal API for testing purposes.
	 */
	public synchronized void discardHandler(Handler<Message<T>> handler) {
		this.discardHandler = handler;
	}

	@Override
	public synchronized MessageConsumer<T> handler(Handler<Message<T>> handler) {
		this.handler = handler;
		if (this.handler != null && !registered) {
			registered = true;
			eventBus.addRegistration(address, this);
		} else if (this.handler == null && registered) {
			// This will set registered to false
			this.unregister();
		}
		return this;
	}

	@Override
	public ReadStream<T> bodyStream() {
		return null; // new BodyReadStream<>(this);
	}

	@Override
	public synchronized boolean isRegistered() {
		return registered;
	}

	@Override
	public synchronized MessageConsumer<T> pause() {
		if (!paused) {
			paused = true;
		}
		return this;
	}

	@Override
	public synchronized MessageConsumer<T> resume() {
		if (paused) {
			paused = false;

		}
		return this;
	}

	@Override
	public synchronized MessageConsumer<T> endHandler(Handler<Void> endHandler) {
		if (endHandler != null) {
			// We should use the HandlerHolder context to properly do this
			// (needs small refactoring)
			Context endCtx = vertx.getOrCreateContext();
			this.endHandler = v1 -> endCtx.runOnContext(v2 -> endHandler.handle(null));
		} else {
			this.endHandler = null;
		}
		return this;
	}

	@Override
	public synchronized MessageConsumer<T> exceptionHandler(Handler<Throwable> handler) {
		return this;
	}

	public Handler<Message<T>> getHandler() {
		return handler;
	}

	public Object getMetric() {
		return metric;
	}
}