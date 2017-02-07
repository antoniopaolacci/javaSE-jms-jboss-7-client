package it.paolacci.scbcd6.client;

import it.paolacci.scbcd6.entity.MyMessage;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

public class MessageDrivenBeanSEClient {
	
	/* Constant(s): */
	private final static String JMS_CONNECTIONFACTORY_JNDI = "java:jms/RemoteConnectionFactory";
	private final static String JMS_JMS_QUEUEDESTINATION_JNDI ="java:/jms/queue/test";
	private static final String PKG_INTERFACES = "org.jboss.ejb.client.naming";
	
	/* Instance variable(s): */
	private ConnectionFactory mQueueConnectionFactory;
	private Queue mQueueDestination;
	private AtomicLong mMessageNumber = new AtomicLong(0);
	
	/**
	 * Looks up the JMS resources required by the client to send JMS messages.
	 */
	private void lookupJmsResources() throws NamingException {
		
		final Properties env = new Properties();
		env.put(Context.INITIAL_CONTEXT_FACTORY, "org.jboss.naming.remote.client.InitialContextFactory");
		env.put(PKG_INTERFACES, true);
		env.put(Context.PROVIDER_URL, "remote://localhost:4447");
		env.put(Context.SECURITY_PRINCIPAL, "testuser");
		env.put(Context.SECURITY_CREDENTIALS, "testpassword");
		Context initialContext = new InitialContext(env);
	    
		System.out.println("*** Starting JMS Resource Lookup...");
		mQueueConnectionFactory = (ConnectionFactory) initialContext.lookup(JMS_CONNECTIONFACTORY_JNDI);
		mQueueDestination = (Queue) initialContext.lookup(JMS_JMS_QUEUEDESTINATION_JNDI);
		System.out.println("*** JMS Resource Loopup Finished.");
	
	}
	
	
	/**
	 * Sends a JMS message with the next message number and increments the message counter.
	 */
	private void sendJmsMessage() throws JMSException {

		MessageProducer theJMSMessageProducer = null;
		Connection theJMSConnection = null;
		
		try {
			/* Retrieve a JMS connection from the queue connection factory. */
			theJMSConnection = mQueueConnectionFactory.createConnection();
			/*
			 * Create the JMS session; not transacted and with auto-
			 * acknowledge.
			 */
			Session theJMSSession = theJMSConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			/* Create a JMS message producer for the queue destination. */
			theJMSMessageProducer = theJMSSession.createProducer(mQueueDestination);
			/* Create the object to be sent in the message created above. */
			MyMessage theObjectToSend = new MyMessage();
			theObjectToSend.setMessageNumber(mMessageNumber.incrementAndGet());
			theObjectToSend.setMessageString("Hello Message Driven Beans");
			theObjectToSend.setMessageTime(new Date());
			/* Create message used to send a Java object. */
			ObjectMessage theJmsObjectMessage = theJMSSession.createObjectMessage();
			theJmsObjectMessage.setObject(theObjectToSend);
			/* Send the message. */
			theJMSMessageProducer.send(theJmsObjectMessage);
		} 
		finally {
			closeJmsResources(theJMSConnection);
		}
	}
	
	
	/**
	 * Closes the supplied JMS connection if it is not null. If supplied connection is null, then do nothing.
	 */
	private void closeJmsResources(Connection inJMSConnection) {
		if (inJMSConnection != null) {
			try{
				inJMSConnection.close();
			} catch (JMSException theException) {
				// Ignore exceptions.
			}
		}
	}
	
	
	/**
	 * Main entry point of the Java SE message driven bean example program.
	 */
	public static void main(String[] args) {
		
		System.out.println("*** Java SE JMS Client started.");
		
		MessageDrivenBeanSEClient theClient = new MessageDrivenBeanSEClient();
		
		try {
			
			theClient.lookupJmsResources();
			
			for (int i = 0; i < 10; i++) {
				theClient.sendJmsMessage();
				System.out.println("### Sent message: " +(i + 1));
			}
			
		} catch (Exception theException) {
			theException.printStackTrace();
		}
		
		System.out.println("*** Java SE JMS Client finished.");
		
		System.exit(0);
	}
	
}