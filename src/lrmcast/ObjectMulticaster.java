package lrmcast;

import java.io.IOException;

import ibis.ipl.Ibis;
import ibis.io.SerializationBase;
import ibis.io.SerializationInput;
import ibis.io.SerializationOutput;
import ibis.ipl.IbisException;
import ibis.ipl.IbisIdentifier;

public class ObjectMulticaster implements ByteArrayReceiver {
   
    private LableRoutingMulticast lrmc; 
    
    private LRMCOutputStream os; 
    private LRMCInputStream is;
    
    private BufferedArrayOutputStream bout;
    private BufferedArrayInputStream bin;
    
    private SerializationOutput sout;
    private SerializationInput sin;    
    
    public ObjectMulticaster(Ibis ibis) throws IOException, IbisException {         
        lrmc = new LableRoutingMulticast(ibis, this);
        
        os = new LRMCOutputStream(lrmc);
        is = new LRMCInputStream();
        
        bout = new BufferedArrayOutputStream(os);
        bin = new BufferedArrayInputStream(is);
        
        sout = SerializationBase.createSerializationOutput("ibis", bout);
        sin = SerializationBase.createSerializationInput("ibis", bin);        
    }

    public void gotMessage(IbisIdentifier receiver, byte[] message) {          
        is.addBuffer(message);
        
        //System.err.println("____ got message(" + message.length + ")");       
    }
    
    public void send(IbisIdentifier [] id, Object o) throws IOException { 
        os.setTarget(id);
        sout.writeObject(o);
        sout.reset(true);
        sout.flush();
    }
    
    public Object receive() throws IOException, ClassNotFoundException { 
        return sin.readObject();
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
