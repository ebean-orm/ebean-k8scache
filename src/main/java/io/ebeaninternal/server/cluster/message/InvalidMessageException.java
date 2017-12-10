package io.ebeaninternal.server.cluster.message;

public class InvalidMessageException extends Exception {

	public int messageKey;

	public InvalidMessageException(int messageKey) {
		super("invalid message key");
		this.messageKey = messageKey;
	}

	public int getMessageKey() {
		return messageKey;
	}

	public String toString() {
		return "K8 invalid message key: " + messageKey;
	}
}
