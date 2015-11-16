package io.vertx.amqp;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageConsumer;

/**
 * @author <a href="mailto:rajith@rajith.lk">Rajith Muditha Attapattu</a>
 */
public class OnDemandSubscribeDemo extends AbstractVerticle {
	@Override
	public void start() throws Exception {
		System.out.println("Subscribing to address : hello-queue");
		final MessageConsumer cons = vertx.eventBus().consumer("hello-queue", message -> System.out.println("Receiver received msg : " + message.body()));
		vertx.setTimer(30000, v -> {
			cons.unregister();
			System.out.println("Unsubscribing to address : hello-queue");
		});			
	}

	public static void main(String[] args) {
		Vertx.vertx().deployVerticle(new OnDemandSubscribeDemo());
	}
}