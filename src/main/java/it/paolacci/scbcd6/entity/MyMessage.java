package it.paolacci.scbcd6.entity;

import java.io.Serializable;

import java.util.Date;


/**
 * Represents a message sent by a producer to the topic.
 */


public class MyMessage implements Serializable {
	/* Constant(s): */
	private static final long serialVersionUID = -4682924711829199796L;
	
	/* Instance variable(s): */
	private String mMessageString;
	private long mMessageNumber;
	private Date mMessageTime;
	
	/* Getter methods */
	
	public String getMessageString() {
		return mMessageString;
	}
	
	public void setMessageString(final String inMessageString) {
		mMessageString = inMessageString;
	}
	
	public long getMessageNumber() {
		return mMessageNumber;
	}
	
	
	/* Setter methods */
	
	public void setMessageNumber(final long inMessageNumber) {
		mMessageNumber = inMessageNumber;
	}
	
	public Date getMessageTime() {
		return mMessageTime;
	}
	
	public void setMessageTime(final Date inMessageTime) {
		mMessageTime = inMessageTime;
	}
	
}