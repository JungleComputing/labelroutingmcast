package lrmcast;

import ibis.ipl.IbisIdentifier;

import java.io.IOException;
import java.io.OutputStream;

public class LRMCOutputStream extends OutputStream {

    private LableRoutingMulticast mcast;
    private IbisIdentifier [] target; 
    
    private boolean closed = false;
    
    LRMCOutputStream(LableRoutingMulticast mcast) { 
        this.mcast = mcast;        
    }

    public void setTarget(IbisIdentifier [] target) { 
        this.target = target; 
    }
    
    public void close() { 
        closed = true;
    }
    
    public void write(byte [] b, int off, int len) {
        
       if (closed) { 
           System.err.println("____ got write(" + off + ", " + len + 
                   ") while closed!");
           return;
       }
        
      // System.err.println("____ got write(" + off + ", " + len + ")");
        
       mcast.send(target, b, off, len);
       
    //   System.err.println("____ done write(" + off + ", " + len + ")");       
    }
    
    public void write(int b) throws IOException {
        // Ouch ... fortunately, it is never used...
        write(new byte[] { (byte) (b & 0xff) }, 0, 1);
    }
}
