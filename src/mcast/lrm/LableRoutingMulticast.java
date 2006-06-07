package mcast.lrm;

import ibis.ipl.Ibis;
import ibis.ipl.IbisException;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.PortType;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import ibis.ipl.ReceivePortIdentifier;
import ibis.ipl.SendPort;
import ibis.ipl.StaticProperties;
import ibis.ipl.WriteMessage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

public class LableRoutingMulticast extends Thread {

    private final static int ZOMBIE_THRESHOLD = 10000;

    private final Ibis ibis;
    private final PortType portType;
    private ReceivePort receive; 
    
    private MessageReceiver receiver;
    
    private final MessageCache cache;
    
    private final HashMap sendports = new HashMap();    
    private final HashMap diedmachines = new HashMap();
    
    private boolean mustStop = false;    
    private boolean changeOrder = false;    
    
    private String [] destinations = null;
    
    /* private Sender sender; 
    
    private class Sender extends Thread { 
        
        public void run() { 
            while (true) { 
                Message m = dequeueSend();
                
                try { 
                    internalSend(m);
                } catch (Exception e) {
                    System.err.println("Sender thread got exception! " + e);
                    e.printStackTrace(System.err);
                }
                
                cache.put(m);
            }
        }        
    }
       */ 
    public LableRoutingMulticast(Ibis ibis, MessageReceiver m, MessageCache c) 
        throws IOException, IbisException {
        this(ibis, m, c, false);
    }
            
    public LableRoutingMulticast(Ibis ibis, MessageReceiver m, MessageCache c, 
            boolean changeOrder) throws IOException, IbisException {
        this.ibis = ibis;    
        this.receiver = m;
        this.cache = c;
        this.changeOrder = changeOrder;
                
        StaticProperties s = new StaticProperties();
        s.add("Serialization", "data");
        s.add("Communication", "ManyToOne, Reliable, ExplicitReceipt");
        
        portType = ibis.createPortType("Ring", s);
        
        receive = portType.createReceivePort("Ring-" + ibis.identifier().name());
        receive.enableConnections();
        
       /* sender = new Sender();
        sender.start();
        */
        
        this.start();
    }
    
    private final String [] getDestinationArray(int len) {
        
        return new String[len];
        
        /*
        if (destinations == null || destinations.length < len) {         
            destinations = new String[len];
        } 
               
        return destinations;
        */
    }
             
    private void receive(ReadMessage rm) { 

        String sender = null;        
        String [] destinations = null;        
        Message message = null;
        
        try {
            sender = rm.readString();
                        
            int dst = rm.readInt();
            
            if (dst > 0) { 
                destinations = getDestinationArray(dst);
                
                for (int i=0;i<dst;i++) { 
                    destinations[i] = rm.readString();
                }
            }
            
            int id = rm.readInt();
            int num = rm.readInt();            
            int len = rm.readInt();        
            
            message = cache.get(len);            
            
            if (len > 0) { 
                rm.readArray(message.buffer, 0, len);
            } 

            rm.finish();
            
            message.set(sender, destinations, id, num, 0, len);
        
            if (dst > 0) { 
                internalSend(message);        
            }
            
        } catch (IOException e) {
            System.err.println("Failed to receive message: " + e);
            e.printStackTrace(System.err);
            rm.finish(e);
            
            if (message != null) { 
                cache.put(message);
            }
            
            return;
        }
                    
        try { 
          //  message.up();
            receiver.gotMessage(message);            
        } catch (Throwable e) {
            System.err.println("Delivery failed! " + e);
            e.printStackTrace(System.err);
        }
    }
              
    private SendPort getSendPort(String id) { 
        SendPort sp = (SendPort) sendports.get(id);
        
        if (sp == null) {
            // We're not connect to this ibis yet, so connect and store for 
            // later use.
            
            // Test if the machine died recently to prevent us from trying to 
            // connect over and over again (this may be a problem since a single
            // large mcast may be fragmented into many small packets, each with 
            // the same route containing the dead machine)            
            Long ripTime = (Long) diedmachines.get(id);

            if (ripTime != null) { 
                
                long now = System.currentTimeMillis();
                
                if (now - ripTime.longValue() > ZOMBIE_THRESHOLD) { 
                    // the machine has been dead for a long time, but the sender
                    // insists it is still alive. Lets try again and see what 
                    // happens.
                    diedmachines.remove(id);
                    
                    System.err.println("Sender insists that " + id  
                            + " is still allive, so I'll try again!");
                } else { 
                    //System.err.println("Ignoring " + id + " since it's dead!");
                    return null;
                }                
            }
            
            
            boolean failed = false;
            
            ReceivePortIdentifier tmp = null; 
                            
            try { 
                sp = portType.createSendPort();
                
                tmp = ibis.registry().lookupReceivePort("Ring-" + id, 1000);
                
                if (tmp != null) {                 
                    sp.connect(tmp, 1000);                
                    sendports.put(id, sp);
                } else { 
                    failed = true;
                }
            } catch (IOException e) {
                failed = true;
            } 
            
            if (failed) {                 
                System.err.println("Failed to connect to " + id 
                        + " - informing nameserver!");
                
                // notify the nameserver that this machine may be dead...
                try { 
                    if (tmp != null) { 
                        ibis.registry().maybeDead(tmp.ibis());
                    } 
                    diedmachines.put(id, new Long(System.currentTimeMillis()));                    
                } catch (Exception e2) {
                    
                    System.err.println("Failed to inform nameserver! " + e2);
                    
                    // ignore
                }
                
                System.err.println("Done informing nameserver");
                                
                return null;
            }
        }
   
        return sp;
    }
    
    private void sendMessage(SendPort sp, int fromDest, Message m) throws IOException { 
        
       // System.err.println("____ sendMessage(" + messageID + ", byte[" + len + "])");
        
        WriteMessage wm = sp.newMessage();
        
        wm.writeString(m.sender);
        
        //wm.writeInt(destinations.length-1);
        wm.writeInt(m.destinations.length-fromDest);
        
        for (int i=fromDest;i<m.destinations.length;i++) { 
            wm.writeString(m.destinations[i]);
        } 
        
        wm.writeInt(m.id);
        
        if (m.last) { 
            wm.writeInt(m.num | Message.LAST_PACKET);
        } else { 
            wm.writeInt(m.num);            
        }
        
        wm.writeInt(m.len);                
        
        if (m.len > 0) { 
            wm.writeArray(m.buffer, m.off, m.len);
        }              
        
        wm.finish();
    }
        
    private void internalSend(Message m) {
      
        if (m.destinations == null || m.destinations.length == 0) { 
            return; 
        }
        
        // Get the next target from the destination array. If this fails, get 
        // the next one, etc. If no working destination is found we give up.  
        int index = 0;        
        SendPort sp = null;
        String id = null;
        
        do { 
            id = m.destinations[index++];
            sp = getSendPort(id);
        } while (sp == null && index < m.destinations.length);
        
        if (sp == null) { 
            // No working destinations where found, so give up!
            return;
        }
        
        try { 
            sendMessage(sp, index, m);
        } catch (IOException e) {
            System.err.println("Write to " + id + " failed!");            
            sendports.remove(id);            
        }
    } 

    public void setDestination(IbisIdentifier [] destinations) { 

        if (changeOrder) { 
            // We are allowed to change the order of machines in the destination
            // array. This can be used to make the mcast 'cluster aware'.            
            IbisSorter.sort(ibis.identifier(), destinations);                     
        }
        
        this.destinations = new String[destinations.length];
        
        for (int i=0;i<destinations.length;i++) { 
            this.destinations[i] = destinations[i].name();
        }
    }
    
    public void send(int id, int num, byte [] message, int off, int len) {
                
        Message m = new Message(ibis.identifier().name(), destinations, id, num,
                message, off, len);
        
        internalSend(m);
    } 

    public void send(Message m) {
        
        if (m.destinations == null) { 
            m.destinations = destinations;
        }
        
        if (m.sender == null) { 
            m.sender = ibis.identifier().name();
        }
        
        internalSend(m);
    } 
    
    public void run() { 

        boolean done = false;
        
        while (!done) {   
            
            try { 
                receive(receive.receive());               
            } catch (Exception e) {
                
                synchronized (this) {
                    if (!mustStop) {                
                        System.err.println("Receive failed! " + e);
                    } 
                } 
            }  
            
            synchronized (this) {
                done = mustStop;
            }
        }    
    }

    public void done() {

        synchronized (this) {
            mustStop = true;
        }
                
        try {             
            receive.disableConnections();
        
            Iterator i = sendports.values().iterator();
        
            while (i.hasNext()) { 
                SendPort tmp = (SendPort) i.next();
                tmp.close();
            }
            
            receive.close(1000);
        } catch (Exception e) {
            // ignore, we tried...
        }
    }
}
