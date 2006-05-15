package lrmcast;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReceivePortIdentifier;
import ibis.ipl.Ibis;
import ibis.ipl.IbisException;
import ibis.ipl.PortType;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import ibis.ipl.SendPort;
import ibis.ipl.StaticProperties;
import ibis.ipl.WriteMessage;

public class LableRoutingMulticast extends Thread {

    private final static int ZOMBIE_THRESHOLD = 10000;
    
    private final Ibis ibis;
    private final PortType portType;
    private ReceivePort receive; 
    
    private ByteArrayReceiver receiver;
    private final HashMap sendports = new HashMap();    
    private final HashMap diedmachines = new HashMap();
    private final HashMap senders = new HashMap();
    
    private boolean mustStop = false;    
    private boolean changeOrder = false;    
    
    private String[] destinations = null;
    
    private byte[] data = null;
    
    public LableRoutingMulticast(Ibis ibis, ByteArrayReceiver m) 
        throws IOException, IbisException {
        this(ibis, m, false);
    }
            
    public LableRoutingMulticast(Ibis ibis, ByteArrayReceiver m, 
            boolean changeOrder) throws IOException, IbisException {
        this.ibis = ibis;    
        this.receiver = m;
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
    
    private final byte [] getByteArray(int len) {
        
        if (data == null || data.length < len) {         
            data = new byte[len];
        }
        
        return data;
    }
    
    private void receive(ReadMessage rm) { 

        String sender = null;        
        String [] destinations = null;
        byte [] message = null;
        int dst;
        
        try {
            sender = rm.readString();
                        
            dst = rm.readInt();
            
            if (dst > 0) { 
                destinations = getDestinationArray(dst);
                
                for (int i=0;i<dst;i++) { 
                    destinations[i] = rm.readString();
                }
            }
            
            int data = rm.readInt();        
            message = getByteArray(data);            
            
            if (data > 0) { 
                rm.readArray(message, 0, data);
            } 

            rm.finish();

            if (dst > 0) { 
                send(sender, destinations, dst, message, 0, message.length);
            }
            
        } catch (Exception e) {
            System.err.println("Failed to receive message: " + e);
            e.printStackTrace(System.err);
            return;
        }
        
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
        
        try { 
            boolean reuse = receiver.gotMessage(senderID, message);
            
            if (!reuse) { 
                data = null;
            }
        } catch (Throwable e) {
            System.err.println("Delivery failed! " + e);
        }
    }
    
    public void send(IbisIdentifier [] destinations, byte [] message) {
        send(destinations, message, 0, message.length);
    } 
    
    
    public void send(IbisIdentifier [] destinations, byte [] message, int off, int len) {
        
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

        send(ibis.identifier().name(), tmp, tmp.length, message, off, len);        
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
                    System.err.println("Ignoring " + id + " since it's dead!");
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
                        ibis.registry().dead(tmp.ibis());
                    } 
                    diedmachines.put(id, new Long(System.currentTimeMillis()));                    
                } catch (Exception e2) {
                    // ignore
                }
                
                return null;
            }
        }
   
        return sp;
    }
    
    private void sendMessage(SendPort sp, String sender, String [] destinations, 
            int fromDest, int toDest, byte [] message, int off, int len) 
            throws IOException { 
        
        WriteMessage wm = sp.newMessage();
        
        wm.writeString(sender);
        
        //wm.writeInt(destinations.length-1);
        wm.writeInt(toDest-fromDest);
        
        for (int i=fromDest;i<toDest;i++) { 
            wm.writeString(destinations[i]);
        } 
        
        wm.writeInt(len);                
        
        if (len > 0) { 
            wm.writeArray(message, off, len);
        }              
        
        wm.finish();
    }
    
    private void send(String sender, String [] destinations,
            int numdest, byte [] message, int off, int len) {
      
        if (destinations == null || destinations.length == 0) { 
            return; 
        }
        
        String id = destinations[0];

        SendPort sp = getSendPort(id);
        
        if (sp == null) { 
            // it's dead Jim!
            return;
        }
        
        try { 
            sendMessage(sp, sender, destinations, 1, destinations.length, 
                    message, off, len);
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
