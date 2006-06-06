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
    
    //private ByteArrayCache cache = new ByteArrayCache();
    
    private final MessageCache cache; // = new BufferCache(1000);
    
    private final HashMap sendports = new HashMap();    
    private final HashMap diedmachines = new HashMap();
    
    private boolean mustStop = false;    
    private boolean changeOrder = false;    
    
    private String[] destinations = null;
    
    //private byte[] data = null;
           
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
        
        this.start();
    }
    
    private final String [] getDestinationArray(int len) {
        
        if (destinations == null || destinations.length < len) {         
            destinations = new String[len];
        } 
               
        return destinations;
    }

    /*
    private final byte [] getByteArray(int len) {
        
        if (data == null || data.length < len) {         
            data = cache.get(len);
        }
        
        return data;
    }
    
    public ByteArrayCache getCache() { 
        return cache;
    }
    */
                         
    private void receive(ReadMessage rm) { 

        String sender = null;        
        String [] destinations = null;        
        Message buffer = null;
        
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
            
            buffer = cache.get(len);            
            
            if (len > 0) { 
                rm.readArray(buffer.buffer, 0, len);
            } 

            rm.finish();
            
            buffer.set(id, num, false, len);

            if (dst > 0) { 
                send(sender, destinations, dst, id, num, buffer.buffer, 0, len);
            }
            
        } catch (IOException e) {
            System.err.println("Failed to receive message: " + e);
            e.printStackTrace(System.err);
            rm.finish(e);
            
            if (buffer != null) { 
                cache.put(buffer);
            }
            
            return;
        }
      /*  
        IbisIdentifier senderID = (IbisIdentifier) senders.get(sender);
        
        if (senderID == null) {
            try {
                senderID = ibis.registry().lookupIbis(sender, 1000);
            } catch (Exception e) {
                System.err.println("Delivery failed! Cannot find sender " + e);
                return;
            }
                        
            senders.put(sender, senderID);
        }
        
        if (senderID == null) { receive
            System.err.println("Delivery failed! Cannot find sender for " + sender);
            return;
        }
        */
        try { 
            boolean reuse = receiver.gotMessage(sender, buffer);
            
            //if (!reuse) { 
//                data = null;
            //}
        } catch (Throwable e) {
            System.err.println("Delivery failed! " + e);
            e.printStackTrace(System.err);
        }
    }
    
    public void send(IbisIdentifier [] destinations, int id, int num, byte [] message) {
        send(destinations, id, num, message, 0, message.length);
    } 
    
    
    public void send(IbisIdentifier [] destinations, int id, int num, 
            byte [] message, int off, int len) {
        
        if (changeOrder) { 
            // We are allowed to change the order of machines in the destination
            // array. This can be used to make the mcast 'cluster aware'.            
            IbisSorter.sort(ibis.identifier(), destinations);
            /*
            System.err.println("Sender " + ibis.identifier() + " " 
                    + ibis.identifier().cluster() + " sorted ids: ");
            
            for (int i=0;i<destinations.length;i++) { 
                System.err.println("  " + destinations[i] + " (" 
                        + destinations[i].cluster() + ")");                
            } 
            */           
        }
        
        String [] tmp = new String[destinations.length];
        
        for (int i=0;i<destinations.length;i++) { 
            tmp[i] = destinations[i].name();
        }

        send(ibis.identifier().name(), tmp, tmp.length, id, num, message, off,
                len);        
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
    
    private void sendMessage(SendPort sp, String sender, String [] destinations, 
            int fromDest, int toDest, int messageID, int messageNum, 
            byte [] message, int off, int len) throws IOException { 
        
       // System.err.println("____ sendMessage(" + messageID + ", byte[" + len + "])");
        
        WriteMessage wm = sp.newMessage();
        
        wm.writeString(sender);
        
        //wm.writeInt(destinations.length-1);
        wm.writeInt(toDest-fromDest);
        
        for (int i=fromDest;i<toDest;i++) { 
            wm.writeString(destinations[i]);
        } 
        
        wm.writeInt(messageID);
        wm.writeInt(messageNum);
        
        wm.writeInt(len);                
        
        if (len > 0) { 
            wm.writeArray(message, off, len);
        }              
        
        wm.finish();
    }
    
    private void send(String sender, String [] destinations,
            int numdest, int messageID, int messageNum, byte [] message, 
            int off, int len) {
      
        if (destinations == null || destinations.length == 0) { 
            return; 
        }
        
        // Get the next target from the destination array. If this fails, get 
        // the next one, etc. If no working destination is found we give up.  
        int index = 0;        
        SendPort sp = null;
        String id = null;
        
        do { 
            id = destinations[index++];
            sp = getSendPort(id);
        } while (sp == null && index < destinations.length);
        
        if (sp == null) { 
            // No working destinations where found, so give up!
            return;
        }
        
        try { 
            sendMessage(sp, sender, destinations, index, destinations.length, 
                    messageID, messageNum, message, off, len);
        } catch (IOException e) {
            System.err.println("Write to " + id + " failed!");            
            sendports.remove(id);            
        }
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
