package lrmcast;

import ibis.ipl.IbisIdentifier;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;

public class LRMCInputStream extends InputStream {
    
    private IbisIdentifier source;
    private LinkedList data = new LinkedList(); 
    
    private byte [] current = null; 
    private int index = 0;
        
    public LRMCInputStream(IbisIdentifier source) { 
        this.source = source;
    }
    
    public IbisIdentifier getSource() { 
        return source;
    }
    
    public boolean haveData() {  
        return (data.size() != 0);
    }
    
    public synchronized void addBuffer(byte [] buffer) { 
        data.addLast(buffer);
        notifyAll();
    }
        
    private synchronized void getBuffer() { 
        
        while (data.size() == 0) {
            try { 
                wait();
            } catch (InterruptedException e) {
                // ignore
            }
        }        
        
        current = (byte []) data.removeFirst();
        index = 0;
    }
    
    public int read(byte b[], int off, int len) throws IOException {
        
        if (current == null || index == current.length) { 
            getBuffer();
        }        
        
        int leftover = current.length-index;
                
        if (leftover <= len) { 
            System.arraycopy(current, index, b, off, leftover);
            current = null;
            index = 0;
            return leftover;
        } else {          
            System.arraycopy(current, index, b, off, len);
            index += len;
            return len;
        } 
    }
    
    public int read() throws IOException {
        // Ouch ... fortunately it's never used! 
        byte [] tmp = new byte[1];        
        read(tmp, 0, 1);               
        return tmp[0];
    }

}
