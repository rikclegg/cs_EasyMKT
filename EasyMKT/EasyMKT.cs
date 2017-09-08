/* Copyright 2017. Bloomberg Finance L.P.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:  The above
 * copyright notice and this permission notice shall be included in all copies
 * or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 */

using System;
using System.Collections.Generic;
using Name = Bloomberglp.Blpapi.Name;
using Session = Bloomberglp.Blpapi.Session;
using Service = Bloomberglp.Blpapi.Service;
using SessionOptions = Bloomberglp.Blpapi.SessionOptions;
using CorrelationID = Bloomberglp.Blpapi.CorrelationID;
using LogLevels = com.bloomberg.mktdata.samples.Log.LogLevels;
using Event = Bloomberglp.Blpapi.Event;
using Message = Bloomberglp.Blpapi.Message;
using Subscription = Bloomberglp.Blpapi.Subscription;

namespace com.bloomberg.mktdata.samples
{
    public class EasyMKT
    {

        // ADMIN
        private static readonly Name   SLOW_CONSUMER_WARNING	        = new Name("SlowConsumerWarning");
        private static readonly Name   SLOW_CONSUMER_WARNING_CLEARED	= new Name("SlowConsumerWarningCleared");

        // SESSION_STATUS
        private static readonly Name   SESSION_STARTED 		    = new Name("SessionStarted");
        private static readonly Name   SESSION_TERMINATED 		= new Name("SessionTerminated");
        private static readonly Name   SESSION_STARTUP_FAILURE  = new Name("SessionStartupFailure");
        private static readonly Name   SESSION_CONNECTION_UP 	= new Name("SessionConnectionUp");
        private static readonly Name   SESSION_CONNECTION_DOWN	= new Name("SessionConnectionDown");

        // SERVICE_STATUS
        private static readonly Name   SERVICE_OPENED 			= new Name("ServiceOpened");
        private static readonly Name   SERVICE_OPEN_FAILURE 	= new Name("ServiceOpenFailure");

        // SUBSCRIPTION_STATUS + SUBSCRIPTION_DATA
        private static readonly Name   SUBSCRIPTION_FAILURE 	= new Name("SubscriptionFailure");
        private static readonly Name   SUBSCRIPTION_STARTED	    = new Name("SubscriptionStarted");
        private static readonly Name   SUBSCRIPTION_TERMINATED	= new Name("SubscriptionTerminated");

        public Securities securities;
        public SubscriptionFields fields;

        private String host;
        private int port;

        Session session;
        Service service;

        Dictionary<CorrelationID, MessageHandler> messageHandlers = new Dictionary<CorrelationID, MessageHandler>();

        private volatile bool ready = false;

        private static readonly String MKTDATA_SERVICE = "//blp/mktdata";
	
	    public EasyMKT()
        {
            this.host = "localhost";
            this.port = 8194;
            initialise();
        }

        public EasyMKT(String host, int port)
        {
            this.host = host;
            this.port = port;
            initialise();
        }

        private void initialise()
        {

            fields = new SubscriptionFields(this);
            securities = new Securities(this);

            SessionOptions sessionOptions = new SessionOptions();
            sessionOptions.ServerHost = this.host;
            sessionOptions.ServerPort = this.port;

            this.session = new Session(sessionOptions, new EventHandler(ProcessEvent));

            try
            {
                this.session.StartAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }

            while (!this.ready) ;

        }

        public SubscriptionField AddField(String fieldName)
        {

            return fields.CreateSubscriptionField(fieldName);
        }

        public Security AddSecurity(String ticker)
        {

            return securities.createSecurity(ticker);
        }

        public void start()
        {
            foreach (Security s in securities)
            {
                this.AddSubscription(s);
            }

        }

        public void ProcessEvent(Event event, Session session) {
            try
            {
                switch (event.eventType().intValue())
                    {                
                    case Event.EventType.Constants.ADMIN:
                        processAdminEvent(event, session);
                        break;
                    case Event.EventType.Constants.SESSION_STATUS:
                        processSessionEvent(event, session);
                        break;
                    case Event.EventType.Constants.SERVICE_STATUS:
                        processServiceEvent(event, session);
                        break;
                    case Event.EventType.Constants.SUBSCRIPTION_DATA:
                        processSubscriptionDataEvent(event, session);
                        break;
                    case Event.EventType.Constants.SUBSCRIPTION_STATUS:
                        processSubscriptionStatus(event, session);
                        break;
                    default:
                        processMiscEvents(event, session);
                        break;
        }
    } catch (Exception e) {
                    e.printStackTrace();
                }
            }

		    private void processAdminEvent(Event event, Session session) throws Exception
    {
        Log.LogMessage(LogLevels.BASIC, "Processing " + event.eventType().toString());

        MessageIterator msgIter = event.messageIterator();
            
			    while (msgIter.hasNext()) {
            Message msg = msgIter.next();
            if (msg.messageType().equals(SLOW_CONSUMER_WARNING))
            {
                Log.LogMessage(LogLevels.BASIC, "Warning: Entered Slow Consumer status");
            }
            else if (msg.messageType().equals(SLOW_CONSUMER_WARNING_CLEARED))
            {
                Log.LogMessage(LogLevels.BASIC, "Slow consumer status cleared");
            }
        }
    }

    private void processSessionEvent(Event event, Session session) throws Exception
    {

        Log.LogMessage(LogLevels.BASIC, "Processing " + event.eventType().toString());

        MessageIterator msgIter = event.messageIterator();
            
			    while (msgIter.hasNext()) {

            Message msg = msgIter.next();

            if (msg.messageType().equals(SESSION_STARTED))
            {
                Log.LogMessage(LogLevels.BASIC, "Session started...");
                session.openServiceAsync(MKTDATA_SERVICE);
            }
            else if (msg.messageType().equals(SESSION_STARTUP_FAILURE))
            {
                Log.LogMessage(LogLevels.BASIC, "Error: Session startup failed");
            }
            else if (msg.messageType().equals(SESSION_TERMINATED))
            {
                Log.LogMessage(LogLevels.BASIC, "Session has been terminated");
            }
            else if (msg.messageType().equals(SESSION_CONNECTION_UP))
            {
                Log.LogMessage(LogLevels.BASIC, "Session connection is up");
            }
            else if (msg.messageType().equals(SESSION_CONNECTION_DOWN))
            {
                Log.LogMessage(LogLevels.BASIC, "Session connection is down");
            }
        }
    }

    private void processServiceEvent(Event event, Session session)
    {

        Log.LogMessage(LogLevels.BASIC, "Processing " + event.eventType().toString());

        MessageIterator msgIter = event.messageIterator();

        while (msgIter.hasNext())
        {

            Message msg = msgIter.next();

            if (msg.messageType().equals(SERVICE_OPENED))
            {

                Log.LogMessage(LogLevels.BASIC, "Service opened...");

                easyMKT.service = session.getService(MKTDATA_SERVICE);

                Log.LogMessage(LogLevels.BASIC, "Got service...ready...");
                this.easyMKT.ready = true;

            }
            else if (msg.messageType().equals(SERVICE_OPEN_FAILURE))
            {
                Log.LogMessage(LogLevels.BASIC, "Error: Service failed to open");
            }
        }
    }

    private void processSubscriptionStatus(Event event, Session session) throws Exception
    {
        Log.LogMessage(LogLevels.BASIC, "Processing " + event.eventType().toString());

        MessageIterator msgIter = event.messageIterator();
            
			    while (msgIter.hasNext()) {
            Message msg = msgIter.next();
            if (msg.messageType().equals(SUBSCRIPTION_STARTED))
            {
                Log.LogMessage(LogLevels.BASIC, "Subscription started successfully: " + msg.correlationID().toString());
            }
            else if (msg.messageType().equals(SUBSCRIPTION_FAILURE))
            {
                Log.LogMessage(LogLevels.BASIC, "Error: Subscription failed: " + msg.correlationID().toString());
            }
            else if (msg.messageType().equals(SUBSCRIPTION_TERMINATED))
            {
                Log.LogMessage(LogLevels.BASIC, "Subscription terminated : " + msg.correlationID().toString());
            }
        }
    }

    private void processSubscriptionDataEvent(Event event, Session session) throws Exception
    {

        Log.LogMessage(LogLevels.DETAILED, "Processing " + event.eventType().toString());

        MessageIterator msgIter = event.messageIterator();
            
        	    while (msgIter.hasNext()) {

            Message msg = msgIter.next();
            // process the incoming market data event
            messageHandlers.get(msg.correlationID()).handleMessage(msg);
        }
    }

    private void processMiscEvents(Event event, Session session) throws Exception
    {
        Log.LogMessage(LogLevels.BASIC, "Processing " + event.eventType().toString());

        MessageIterator msgIter = event.messageIterator();
            
			    while (msgIter.hasNext()) {
            Message msg = msgIter.next();
            Log.LogMessage(LogLevels.BASIC, "MESSAGE: " + msg);
        }
    }

        }

	    public void AddSubscription(Security security) {

        Log.LogMessage(LogLevels.DETAILED, "Adding subscription for security: " + security.getName());

        CorrelationID cID = new CorrelationID(security.getName());

        Subscription newSubscription = new Subscription(security.getName(), fields.getFieldList(), "", cID);

        Log.LogMessage(LogLevels.DETAILED, "Topic string: " + newSubscription.subscriptionString());

        SubscriptionList newSubList = new SubscriptionList();

        newSubList.add(newSubscription);

        messageHandlers.put(cID, security);

        try
        {
            Log.LogMessage(LogLevels.DETAILED, "Subscribing...");
            this.session.subscribe(newSubList);
            Log.LogMessage(LogLevels.DETAILED, "Subscription request sent...");
        }
        catch (IOException e)
        {
            Log.LogMessage(LogLevels.BASIC, "Failed to subscribe: " + newSubList.toString());
            e.printStackTrace();
        }

    }	

    }
}