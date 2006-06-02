package mcast.object2;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;

import mcast.lrm.ByteArrayReceiver;
import mcast.lrm.LableRoutingMulticast;

import ibis.ipl.Ibis;
import ibis.io.SerializationBase;
import ibis.io.SerializationInput;
import ibis.io.SerializationOutput;
import ibis.ipl.IbisException;
import ibis.ipl.IbisIdentifier;

public class ObjectMulticaster implements ByteArrayReceiver, ObjectReceiver {
   
    private LableRoutingMulticast lrmc; 
    
    private LRMCOutputStream os; 
    
    private BufferedArrayOutputStream bout;
    private BufferedArrayInputStream bin;
    
    private SerializationOutput sout;
    private SerializationInput sin;    
        
    private HashMap inputStreams = new HashMap(); 
    private LinkedList available = new LinkedList();
    
    private LinkedList objects = new LinkedList();
        
    private long totalData = 0;
    
    public ObjectMulticaster(Ibis ibis) throws IOException, IbisException { 
        this(ibis, false);
    }
    
    public ObjectMulticaster(Ibis ibis, boolean changeOrder) 
        throws IOException, IbisException {
                
        lrmc = new LableRoutingMulticast(ibis, this, changeOrder);
        
        os = new LRMCOutputStream(lrmc);
        
        bout = new BufferedArrayOutputStream(os);
        bin = new BufferedArrayInputStream(null);
        
        sout = SerializationBase.createSerializationOutput("ibis", bout);
        sin = SerializationBase.createSerializationInput("ibis", bin);        
    }
    
    public synchronized boolean gotMessage(String sender, int id, int num, 
            byte[] message) {
        
        LRMCInputStream tmp = (LRMCInputStream) inputStreams.get(sender);
        
        if (tmp == null) {
            tmp = new LRMCInputStream(sender, this);
            inputStreams.put(sender, tmp);           
            available.addLast(sender);
            notifyAll();
        }
        
        tmp.addBuffer(id, num, message);        
        
      // System.err.println("____ got message from " + sender);
        
        return false;
    }
    
    public long send(IbisIdentifier [] id, Object o) throws IOException {
        
        // We only want to return the number of bytes written in this bcast, so 
        // reset the count.
        bout.resetBytesWritten();
        
        // set the target ibises
        os.setTarget(id);
        
        // write the object and reset the stream
        sout.writeObject(o);
        sout.reset(true);      
        sout.flush();

        totalData += bout.bytesWritten();
        
        return bout.bytesWritten();
    }
    
    private synchronized void returnStreams(LRMCInputStream stream) { 

        if (stream.haveData()) { 
            // there is still some data left in the stream, so return it to 
            // the 'available' list. 
            available.addLast(stream.getSource());            
        } else { 
            inputStreams.remove(stream.getSource());
        }
    }
        
    public synchronized Object receive() throws IOException, ClassNotFoundException {
        
        while (objects.size() == 0) { 
            try { 
                wait();
            } catch (Exception e) {
                // TODO: handle exception
            }
        }
        
        return objects.removeFirst();
    }
    
    public long bytesRead() { 
        return bin.bytesRead();
    }
    
    public long bytesWritten() { 
        return bout.bytesWritten();
    }
    
    public long totalBytes() { 
        return totalData;
    }
    
    public void done() {
        try {
            os.close();
            
            //sout.close(); // don't close this one. It keeps on talking...
            sin.close();
            
            lrmc.done();
        } catch (IOException e) {
            // ignore, we tried ...
        }
    }

    public synchronized void haveObject(LRMCInputStream stream) {

        Object result = null; 
        boolean succes = true;
        
        // Plug it into the deserializer
        bin.setInputStream(stream);
        bin.resetBytesRead();
        
        // Read an object
        try { 
            result = sin.readObject();
        } catch (Exception e) {
            succes = false;
        }
        
        // Return the stream to the queue (if necessary) 
        returnStreams(stream);     
        totalData += bin.bytesRead();

        if (succes) { 
            objects.addLast(result);
            notifyAll();
        }       
    }
}
