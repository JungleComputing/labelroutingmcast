package mcast.lrm;

public class MessageCache {

    private final int MAX_MESSAGE_SIZE = 10*1024;
    private final int MAX_SIZE;
    
    private Message cache;
    private int size;
    
   // private long memoryUsage = 0;     
   // private int lowBound = 0;
   // private int highBound = 1024*1024;
      
    public MessageCache(int max) { 
        this.MAX_SIZE = max;
    }
    
    public synchronized void put(Message b) { 
        
        if (size < MAX_SIZE && b.buffer.length == MAX_MESSAGE_SIZE) {
            b.next = cache;
            cache = b;
            size++;
        } else {      
            System.err.println("EEK: got buffer of " + b.buffer.length);
            
            b.next = null;
        }
    }
    
    public synchronized Message get(int len) { 
        
        if (size == 0 || len > MAX_MESSAGE_SIZE) { 
            if (len <= MAX_MESSAGE_SIZE) {
                return new Message(MAX_MESSAGE_SIZE);
            } else { 
                return new Message(len);
            }
        } 
        
        Message tmp = cache;
        cache = cache.next;
        tmp.next = null;
        size--;
        
        return tmp;        
    }    
}
