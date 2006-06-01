package mcast.object2;

import ibis.ipl.IbisIdentifier;

import java.io.IOException;
import java.io.OutputStream;

import mcast.lrm.LableRoutingMulticast;

public class LRMCOutputStream extends OutputStream implements LRMCStreamConstants {

    private LableRoutingMulticast mcast;
    private IbisIdentifier [] target; 
    
    private int currentID = 0;  
    
    private boolean closed = false;    
    private boolean firstPacket = false;
    
    LRMCOutputStream(LableRoutingMulticast mcast) { 
        this.mcast = mcast;        
    }

    public void setTarget(IbisIdentifier [] target) { 
        this.target = target; 
        firstPacket = true;
        currentID++;        
    }

    public void close() { 
        closed = true;
    }
    
    public void write(byte [] b, int off, int len, boolean lastPacket) {
        
       if (closed) { 
           System.err.println("____ got write(" + off + ", " + len + 
                   ") while closed!");
           return;
       }
        
//       System.err.println("____ got write(" + currentID + ", byte[" + len + "])");
        
       int id = currentID;
       
       if (firstPacket) {
           firstPacket = false;
           id = id | FIRST_PACKET;
       }
       
       if (lastPacket) { 
           id = id | LAST_PACKET;
       }
       
       mcast.send(target, id, b, off, len);
       
    //   System.err.println("____ done write(" + off + ", " + len + ")");       
    }
    
    public void write(int b) throws IOException {
        // Ouch ... fortunately, it is never used...
        write(new byte[] { (byte) (b & 0xff) }, 0, 1);
    }
}
