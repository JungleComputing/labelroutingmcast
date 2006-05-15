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
        
    private boolean mustStop = false;    
    private boolean changeOrder = false;    
    
    private IbisIdentifier[] destinations = null;
    
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
    
    private final IbisIdentifier [] getDestinationArray(int len) {
        
        if (destinations == null || destinations.length < len) {         
            destinations = new IbisIdentifier[len];
        } 
        
        return destinations;
    }
    
    private final byte [] getByteArray(int len) { 
        return new byte[len];
    }
    
    private void receive(ReadMessage rm) { 
        
        IbisIdentifier sender = null;
        IbisIdentifier [] destinations = null;
        byte [] message = null;
        int dst;
        
        try {
            sender = (IbisIdentifier) rm.readObject();
            
            dst = rm.readInt();
            
            if (dst > 0) { 
                destinations = getDestinationArray(dst);
                rm.readArray(destinations, 0, dst);
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
        
        
        try { 
            receiver.gotMessage(sender, message);
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
                        
        send(ibis.identifier(), destinations, destinations.length, message, 
                off, len);
    } 
    
    private void send(IbisIdentifier sender, IbisIdentifier [] destinations,
            int numdest, byte [] message, int off, int len) {
      
        if (destinations == null || destinations.length == 0) { 
            return; 
        }
        
        IbisIdentifier id = destinations[0];

        SendPort sp = (SendPort) sendports.get(id);
        
        if (sp == null) {
            // Where not connect to this ibis yet, so connect and store for 
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
                    return;
                }                
            }
            
            
            boolean failed = false;
            
            try { 
                sp = portType.createSendPort();
                
                ReceivePortIdentifier tmp = 
                    ibis.registry().lookupReceivePort("Ring-" + id.name(), 1000);
                
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
                    ibis.registry().dead(id);
                    diedmachines.put(id, new Long(System.currentTimeMillis()));                    
                } catch (Exception e2) {
                    // ignore
                }
                
                return;
            }
        }
        
        try {            
            WriteMessage wm = sp.newMessage();

            wm.writeObject(sender);
            
            wm.writeInt(destinations.length-1);
            
            if (destinations.length > 1) {             
                wm.writeArray(destinations, 1, destinations.length-1);
            } 
            
            wm.writeInt(len);                
            
            if (len > 0) { 
                wm.writeArray(message, off, len);
            }              
            
            wm.finish();
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
