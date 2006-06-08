package mcast.object;

import ibis.io.SerializationBase;
import ibis.io.SerializationInput;
import ibis.io.SerializationOutput;
import ibis.ipl.Ibis;
import ibis.ipl.IbisException;
import ibis.ipl.IbisIdentifier;

import java.io.IOException;
import java.util.LinkedList;

import mcast.lrm.LableRoutingMulticast;
import mcast.lrm.Message;
import mcast.lrm.MessageCache;
import mcast.lrm.MessageReceiver;

public class ObjectMulticaster implements MessageReceiver, ObjectReceiver {
   
    private LableRoutingMulticast lrmc; 
    
    private LRMCOutputStream os; 
    
    private BufferedArrayOutputStream bout;
    private BufferedArrayInputStream bin;
    
    private SerializationOutput sout;
    private SerializationInput sin;    
          
    private final boolean signal;
    private final LinkedList objects = new LinkedList();
    
    private long totalData = 0;
    
    private MessageCache cache; 
    
    private Inputstreams inputStreams = new Inputstreams();
               
    public ObjectMulticaster(Ibis ibis) throws IOException, IbisException { 
        this(ibis, false, false);
    }
    
    public ObjectMulticaster(Ibis ibis, boolean changeOrder, boolean signal) 
        throws IOException, IbisException {
                
        this.signal = signal;
        
        cache = new MessageCache(1000);
                
        lrmc = new LableRoutingMulticast(ibis, this, cache, changeOrder);
        
        os = new LRMCOutputStream(lrmc);

        bout = new BufferedArrayOutputStream(os);
        bin = new BufferedArrayInputStream(null);
        
        sout = SerializationBase.createSerializationOutput("ibis", bout);
        sin = SerializationBase.createSerializationInput("ibis", bin);
    }

    public void setDestination(IbisIdentifier [] dest) { 
        lrmc.setDestination(dest);
    }
    
    public boolean gotMessage(Message m) {
        
        LRMCInputStream tmp = (LRMCInputStream) inputStreams.find(m.sender);
        
        if (tmp == null) {            
            if (signal) { 
                tmp = new LRMCInputStream(m.sender, cache, this);
            } else { 
                tmp = new LRMCInputStream(m.sender, cache);
            }
            
            inputStreams.add(tmp);
        } 
          
        // If the stream did not have data yet, we add it to the 'streams with 
        // data available' list (since it does have data now).
        //if (!tmp.haveData()) {
          
        //} 
        
        tmp.addMessage(m);         
        inputStreams.hasData(tmp);
        
        return false;
    }
    
    public long send(IbisIdentifier [] id, Object o) throws IOException {
        
        // We only want to return the number of bytes written in this bcast, so 
        // reset the count.
        bout.resetBytesWritten();
                
        lrmc.setDestination(id);
        os.reset();
        
        // write the object and reset the stream
        sout.reset(true);              
        sout.writeObject(o);
        sout.flush();

        totalData += bout.bytesWritten();
        
        return bout.bytesWritten();
    }
   
    private Object explicitReceive() throws IOException, ClassNotFoundException {
        
        Object result = null; 
        IOException ioe = null;
        ClassNotFoundException cnfe = null;
        
        // Get the next stream that has some data
        LRMCInputStream stream = inputStreams.getNextFilledStream(); 
        
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
        inputStreams.returnStream(stream);
     
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
    
    private synchronized Object implicitReceive() { 
        while (objects.size() == 0) { 
            try { 
                wait();
            } catch (Exception e) {
                // TODO: handle exception
            }
        }
            
        return objects.removeFirst();
    }
    
    public Object receive() throws IOException, ClassNotFoundException {
    
        if (signal) { 
            return implicitReceive();
        } else { 
            return explicitReceive();
        }        
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
        inputStreams.returnStream(stream);     
        totalData += bin.bytesRead();

        if (succes) { 
            objects.addLast(result);
            notifyAll();
        }       
    }    
}
