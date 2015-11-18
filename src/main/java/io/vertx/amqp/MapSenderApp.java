package io.vertx.amqp;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;

/**
 * @author <a href="mailto:rajith@rajith.lk">Rajith Muditha Attapattu</a>
 */
public class MapSenderApp extends AbstractVerticle {
	String address;

	MapSenderApp(String address) {
		this.address = address;
	}

	@Override
	public void start() throws Exception {
		final AtomicInteger counter = new AtomicInteger();
		Map<String, String> map = new HashMap<String, String>();
		map.put("price", "10");
		map.put("count", "100");
		map.put("message #", String.valueOf(counter.incrementAndGet()));
		vertx.setPeriodic(5000, v -> vertx.eventBus().send(address, map));
	}

	public static void main(String[] args) {
		Vertx.vertx().deployVerticle(new MapSenderApp(args.length == 0 ? "my-queue" : args[0]));
	}
}