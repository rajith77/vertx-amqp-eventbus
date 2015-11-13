package io.vertx.amqp;

import io.vertx.core.VertxOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.impl.HAManager;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.spi.EventBusFactory;
import io.vertx.core.spi.cluster.ClusterManager;

public class AMQPEventBusFactory implements EventBusFactory
{

    @Override
    public EventBus createEventBus(VertxInternal vertx, VertxOptions options, ClusterManager cm, HAManager hm)
    {
    	return new AMQPEventBus(vertx);
    }

}
