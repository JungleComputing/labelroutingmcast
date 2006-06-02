package mcast.object;

import ibis.ipl.IbisIdentifier;

import java.io.IOException;
import java.io.OutputStream;

import mcast.lrm.LableRoutingMulticast;

public class LRMCOutputStream extends OutputStream implements LRMCStreamConstants {

    private LableRoutingMulticast mcast;
    private IbisIdentifier [] target; 
    
    private int currentID = 0;  
    private int currentNUM = 0;  
        
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
        
       if (firstPacket) {
           firstPacket = false;
           currentNUM = 0;
       } else { 
           currentNUM++;
       }
       
       if (lastPacket) { 
           mcast.send(target, currentID++, currentNUM | LAST_PACKET, b, off, len);
       } else { 
           mcast.send(target, currentID++, currentNUM, b, off, len);
       }
    //   System.err.println("____ done write(" + off + ", " + len + ")");       
    }
    
    public void write(int b) throws IOException {
        // Ouch ... fortunately, it is never used...
        write(new byte[] { (byte) (b & 0xff) }, 0, 1);
    }
}
