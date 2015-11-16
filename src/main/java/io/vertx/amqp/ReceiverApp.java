package io.vertx.amqp;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;

/**
 * @author <a href="mailto:rajith@rajith.lk">Rajith Muditha Attapattu</a>
 */
public class ReceiverApp extends AbstractVerticle {
	
	String address;
	
	ReceiverApp(String address){ this.address = address;}
	
	@Override
	public void start() throws Exception {
		vertx.eventBus().consumer(address, message -> System.out.println("Receiver received msg : " + message.body()));
		System.out.println("Receiver ready to receive messages from address : " + address);
	}

	public static void main(String[] args) {
		Vertx.vertx().deployVerticle(new ReceiverApp(args.length == 0 ? "my-queue" : args[0]));
	}
}