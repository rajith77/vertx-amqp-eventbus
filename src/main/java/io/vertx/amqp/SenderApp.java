package io.vertx.amqp;

import java.util.concurrent.atomic.AtomicInteger;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;

/**
 * @author <a href="mailto:rajith@rajith.lk">Rajith Muditha Attapattu</a>
 */
public class SenderApp extends AbstractVerticle {
	String address;

	SenderApp(String address) {
		this.address = address;
	}

	@Override
	public void start() throws Exception {
		final AtomicInteger counter = new AtomicInteger();
		vertx.setPeriodic(5000, v -> vertx.eventBus().send(address, "hello world - " + counter.incrementAndGet()));
	}

	public static void main(String[] args) {
		Vertx.vertx().deployVerticle(new SenderApp(args.length == 0 ? "my-queue" : args[0]));
	}
}