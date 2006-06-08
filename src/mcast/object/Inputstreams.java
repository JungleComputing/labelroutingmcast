package mcast.object;

import java.util.HashMap;
import java.util.LinkedList;

public class Inputstreams {

    private HashMap inputStreams = new HashMap();     
    
    private LinkedList available = new LinkedList();   
    private HashMap hasData = new HashMap();     
    
    public synchronized void add(LRMCInputStream is) {         
        inputStreams.put(is.getSource(), is);        
    }
        
    public synchronized LRMCInputStream find(String sender) {         
        return (LRMCInputStream) inputStreams.get(sender);        
    }
    
    public void returnStream(LRMCInputStream is) {        
        if (is.haveData()) {
            hasData(is);
        }        
    }    
    
    public synchronized void hasData(LRMCInputStream is) {
        
        String sender = is.getSource();
        
        if (!hasData.containsKey(sender)) {
            hasData.put(sender, sender);
            available.addLast(sender);
            notifyAll();
        } 
    }
    
    public synchronized LRMCInputStream getNextFilledStream() {

        while (available.size() == 0) { 
            try { 
                wait();
            } catch (Exception e) {
                // ignore
            }            
        }
        
        String tmp = (String) hasData.remove(available.removeFirst());        
        return (LRMCInputStream) inputStreams.get(tmp);
    }
}
