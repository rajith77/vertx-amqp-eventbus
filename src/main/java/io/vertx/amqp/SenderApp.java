package io.vertx.amqp;

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
		vertx.setPeriodic(5000, v -> vertx.eventBus().send(address, "hello world"));
	}

	public static void main(String[] args) {
		Vertx.vertx().deployVerticle(new SenderApp(args.length == 0 ? "my-queue" : args[0]));
	}
}