package mcast.object;

import java.io.IOException;
import java.io.OutputStream;

import mcast.lrm.LableRoutingMulticast;
import mcast.lrm.Message;

public class LRMCOutputStream extends OutputStream {

    private LableRoutingMulticast mcast;
    
    private int currentID = 1;  
    private int currentNUM = 0;  
        
    private boolean closed = false;    
    private boolean firstPacket = false;
    
    LRMCOutputStream(LableRoutingMulticast mcast) { 
        this.mcast = mcast;        
    }

    public void reset() { 
        firstPacket = true;
    }

    public void close() { 
        closed = true;
    }
    
    public byte [] write(byte [] b, int off, int len, boolean lastPacket) {
        
       if (closed) { 
           System.err.println("____ got write(" + off + ", " + len + 
                   ") while closed!");
           return b;
       }
               
       if (firstPacket) {
           firstPacket = false;
           currentNUM = 0;
       } else { 
           currentNUM++;
       }
       
      // System.err.println("____ got write(" + currentID + " " + currentNUM 
       //       + ", byte[" + len + "] " + firstPacket + " " + lastPacket + ")");
              
       if (lastPacket) { 
           mcast.send(currentID++, currentNUM|Message.LAST_PACKET, b, off, len);
       } else { 
           mcast.send(currentID, currentNUM, b, off, len);
       }
       
       return b; // new byte[b.length];
    }
    
    public void write(int b) throws IOException {
        // Ouch ... fortunately, it is never used...
        write(new byte[] { (byte) (b & 0xff) }, 0, 1);
    }
}
