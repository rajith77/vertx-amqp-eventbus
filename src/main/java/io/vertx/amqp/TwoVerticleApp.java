package io.vertx.amqp;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;

/**
 * @author <a href="mailto:rajith@rajith.lk">Rajith Muditha Attapattu</a>
 */
public class TwoVerticleApp {
	
	TwoVerticleApp(final String address){ 
		final Vertx vertx = Vertx.vertx();
		vertx.deployVerticle(new AbstractVerticle(){
			@Override
			public void start() throws Exception {
				vertx.eventBus().consumer(address, message -> System.out.println("Verticle1 received msg : " + message.body()));
				System.out.println("Verticle1 ready to receive messages from address : " + address);
				
				vertx.deployVerticle(new AbstractVerticle(){
					@Override
					public void start() throws Exception {
						vertx.eventBus().consumer(address, message -> System.out.println("Verticle2 received msg : " + message.body()));
						System.out.println("Verticle2 ready to receive messages from address : " + address);
					}
				});
			}
		});
	}
	
	public static void main(String[] args) {
		TwoVerticleApp app = new TwoVerticleApp(args.length == 0 ? "my-queue" : args[0]);
	}
}