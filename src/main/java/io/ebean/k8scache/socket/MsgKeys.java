package io.ebean.k8scache.socket;

public interface MsgKeys {

	/**
	 * Used to identify client on connection initiation.
	 */
	int HELLO = 182;

//	/**
//	 * Used to confirm protocol on reading messages.
//	 */
//	int MESSAGE_KEY = 183;

	/**
	 * Used to confirm protocol on reading messages.
	 */
	int HEADER = 11;

	/**
	 * Used to confirm protocol on reading messages.
	 */
	int DATA = 12;
}
