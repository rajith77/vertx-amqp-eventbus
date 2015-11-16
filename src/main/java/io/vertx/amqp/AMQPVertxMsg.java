package io.vertx.amqp;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;

class AMQPVertxMsg<T> implements Message<T> {

	org.apache.qpid.proton.message.Message amqpMsg;
    T body;
	
	AMQPVertxMsg(org.apache.qpid.proton.message.Message m, T payload) {
		amqpMsg = m;
		body = payload;
	}

	@Override
	public T body() {
		return body;
	}
	
	@Override
	public String address() {
		return amqpMsg.getAddress();
	}

	@Override
	public void fail(int arg0, String arg1) {
		// TODO Auto-generated method stub
	}

	@Override
	public MultiMap headers() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void reply(Object arg0) {
		// TODO Auto-generated method stub
	}

	@Override
	public <R> void reply(Object arg0, Handler<AsyncResult<Message<R>>> arg1) {
		// TODO Auto-generated method stub
	}

	@Override
	public void reply(Object arg0, DeliveryOptions arg1) {
		// TODO Auto-generated method stub
	}

	@Override
	public <R> void reply(Object arg0, DeliveryOptions arg1, Handler<AsyncResult<Message<R>>> arg2) {
		// TODO Auto-generated method stub
	}

	@Override
	public String replyAddress() {
		return amqpMsg.getReplyTo();
	}
}