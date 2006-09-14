package mcast.object;

import ibis.io.SerializationBase;
import ibis.io.SerializationInput;
import ibis.io.SerializationOutput;

import ibis.ipl.Ibis;
import ibis.ipl.IbisException;
import ibis.ipl.IbisIdentifier;

import java.io.IOException;
import java.io.InterruptedIOException;

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

    private boolean finish = false;
    private boolean receiverDone = false;
    private Thread receiver = null;
    
    private Inputstreams inputStreams = new Inputstreams();
    
    private boolean destinationSet = false;
    private IbisIdentifier [] destination = null; 
    
    public ObjectMulticaster(Ibis ibis, String name) 
        throws IOException, IbisException {       
        this(ibis, false, false, name);
    }
    
    public ObjectMulticaster(Ibis ibis, boolean changeOrder, boolean signal, 
            String name) throws IOException, IbisException {
                
        this.signal = signal;
        
        cache = new MessageCache(1500, 8*1024);
                
        lrmc = new LableRoutingMulticast(ibis, this, cache, changeOrder, name);
        
        os = new LRMCOutputStream(lrmc, cache);

        bout = new BufferedArrayOutputStream(os);
        bin = new BufferedArrayInputStream(null);
        
        sout = SerializationBase.createSerializationOutput("ibis", bout);
        sin = SerializationBase.createSerializationInput("ibis", bin);
    }

    public synchronized void setDestination(IbisIdentifier [] dest) {         
        destination = dest;
    }
    
    public void addIbis(IbisIdentifier id) { 
        lrmc.addIbis(id);
    }
    
    public void removeIbis(IbisIdentifier id) { 
        lrmc.removeIbis(id);
    }    
        
    public boolean gotMessage(Message m) {

        LRMCInputStream tmp;

        // Fix: combined find call and add call into get().
        // There was a race here. (Ceriel)
        if (signal) {
            tmp = inputStreams.get(m.sender, cache, this);
        } else {
            tmp = inputStreams.get(m.sender, cache, null);
        }

        // tmp.addMessage(m);         
        // inputStreams.hasData(tmp);
        // Fix: avoid race: message may have been read before setting
        // hasData. (Ceriel)
        return inputStreams.hasData(tmp, m);
    }
    
    public long send(IbisIdentifier [] id, Object o) throws IOException {
        setDestination(id);
        return send(o);
    }
    
    public synchronized long send(Object o) throws IOException {

        // check if a new destination array is available....
        synchronized (this) {
            if (destination != null) {
                lrmc.setDestination(destination);
                destination = null;
                destinationSet = true;            
            } else if (!destinationSet) { 
                throw new IOException("No destination set!");
            }            
        }
        
        // We only want to return the number of bytes written in this bcast, so 
        // reset the count.
        bout.resetBytesWritten();               
        
        os.reset();
        
        // write the object and reset the stream
        sout.reset(true);              
        sout.writeObject(o);
        sout.flush();

        bout.forcedFlush();
        
        totalData += bout.bytesWritten();
        
        return bout.bytesWritten();
    }
    
    private Object explicitReceive() throws IOException, ClassNotFoundException {
        
        Object result = null; 
        IOException ioe = null;
        ClassNotFoundException cnfe = null;

        // Get the next stream that has some data
        LRMCInputStream stream = inputStreams.getNextFilledStream(); 

        if (stream == null) {
            throw new DoneException("Someone wants us to stop");
        }

        // Plug it into the deserializer
        bin.setInputStream(stream);
        bin.resetBytesRead();
        
        // Read an object
        try { 
            result = sin.readObject();
        } finally {
            inputStreams.returnStream(stream);
            totalData += bin.bytesRead();
        }

        return result;
    }
    
    private synchronized Object implicitReceive() throws IOException { 
        while (objects.size() == 0) { 
            try { 
                wait(2000);
            } catch (InterruptedException e) {
                throw new DoneException("Someone wants us to stop ...");
            }
            if (Thread.currentThread().interrupted()) {
                throw new DoneException("Someone wants us to stop ...");
            }
        }

        return objects.removeFirst();
    }
    
    public Object receive() throws IOException, ClassNotFoundException {
        synchronized(this) {
            if (finish) {
                throw new DoneException("Someone wants us to stop ...");
            }
            receiverDone = false;
            receiver = Thread.currentThread();
        }
        Object o;
        try {
            o = signal ? implicitReceive() : explicitReceive();
        } finally {
            synchronized(this) {
                receiverDone = true;
                if (finish) {
                    notifyAll();
                    throw new DoneException("Someone wants us to stop ...");
                }
                receiver = null;
            }
        }
        return o;
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
        synchronized(this) {
            finish = true;
            // we can interrupt the receiver thread, but we don't know that
            // it will actually finish, so we cannot join it.
            if (receiver != null) {
                receiver.interrupt();
                // Wait until this is noticed.
                while (! receiverDone) {
                    try {
                        wait();
                    } catch(Exception e) {
                        // What to do here? (TODO)
                    }
                }
            }
        }
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

        if (finish) {
            return;
        }

        // Plug it into the deserializer
        bin.setInputStream(stream);
        bin.resetBytesRead();
        
        // Read an object
        try { 
            result = sin.readObject();
        } catch (Exception e) {
            succes = false;
        } finally {
            // Return the stream to the queue (if necessary) 
            inputStreams.returnStream(stream);     
            totalData += bin.bytesRead();
        }

        if (succes) { 
            objects.addLast(result);
            notifyAll();
        }       
    }    
}
