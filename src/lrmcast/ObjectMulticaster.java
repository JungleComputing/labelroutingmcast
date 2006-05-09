package lrmcast;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;

import ibis.ipl.Ibis;
import ibis.io.SerializationBase;
import ibis.io.SerializationInput;
import ibis.io.SerializationOutput;
import ibis.ipl.IbisException;
import ibis.ipl.IbisIdentifier;

public class ObjectMulticaster implements ByteArrayReceiver {
   
    private LableRoutingMulticast lrmc; 
    
    private LRMCOutputStream os; 
    
    private BufferedArrayOutputStream bout;
    private BufferedArrayInputStream bin;
    
    private SerializationOutput sout;
    private SerializationInput sin;    
        
    private HashMap inputStreams = new HashMap(); 
    private LinkedList available = new LinkedList();
    
    private long totalData = 0;
        
    public ObjectMulticaster(Ibis ibis) throws IOException, IbisException {         
        lrmc = new LableRoutingMulticast(ibis, this);
        
        os = new LRMCOutputStream(lrmc);
        
        bout = new BufferedArrayOutputStream(os);
        bin = new BufferedArrayInputStream(null);
        
        sout = SerializationBase.createSerializationOutput("ibis", bout);
        sin = SerializationBase.createSerializationInput("ibis", bin);        
    }

    public synchronized void gotMessage(IbisIdentifier sender, byte[] message) {
        
        LRMCInputStream tmp = (LRMCInputStream) inputStreams.get(sender);
        
        if (tmp == null) {
            tmp = new LRMCInputStream(sender);
            inputStreams.put(sender, tmp);           
            available.addLast(sender);
            notifyAll();
        }
        
        tmp.addBuffer(message);        
        //System.err.println("____ got message(" + message.length + ")");       
    }
    
    public long send(IbisIdentifier [] id, Object o) throws IOException {
        
        // We only want to return the number of bytes written in this bcast, so 
        // reset the count.
        bout.resetBytesWritten();
        
        os.setTarget(id);
        sout.writeObject(o);
        sout.reset(true);
        sout.flush();

        totalData += bout.bytesWritten();
        
        return bout.bytesWritten();
    }
   
    private synchronized LRMCInputStream getNextFilledStream() {

        while (available.size() == 0) { 
            try { 
                wait();
            } catch (Exception e) {
                // ignore
            }            
        }
        
        return (LRMCInputStream) inputStreams.get(available.removeFirst());
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
        
    public Object receive() throws IOException, ClassNotFoundException {
        
        Object result = null; 
        IOException ioe = null;
        ClassNotFoundException cnfe = null;
        
        // Get the next stream that has some data
        LRMCInputStream stream = getNextFilledStream(); 
        
        // Plug it into the deserializer
        bin.setInputStream(stream);
        bin.resetBytesRead();
        
        // Read an object
        try { 
            result = sin.readObject();
        } catch (IOException e1) {
            ioe = e1;
        } catch (ClassNotFoundException e2) {
            cnfe = e2;
        }
        
        // Return the stream to the queue (if necessary) 
        returnStreams(stream);
     
        totalData += bin.bytesRead();
                
        // return the 'result' (exception or otherwise...)
        if (ioe != null) { 
            throw ioe;
        }        
        
        if (cnfe != null) { 
            throw cnfe;
        }                        
        
        return result;
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
}
