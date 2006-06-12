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
    
    public synchronized void put(Message m) { 
        
        // if (m.down() > 0) {
            // message is still used somewhere!
       //     return;
       // }
         
        if (size < MAX_SIZE && m.buffer.length == MAX_MESSAGE_SIZE) {
            m.next = cache;
            cache = m;
            size++;
            
            //System.err.println("Adding message to cache! " + size + " " + 
            //        m.buffer.length);
        } else {      
            //System.err.println("Dropping message from cache! " + size + " " + 
            //        m.buffer.length);
            
            m.next = null;
        }        
    }
    
    public synchronized Message get(String sender, String [] destinations, 
            int id, int num, byte [] message, int off, int len) { 
        
        if (size == 0) { 
            return new Message(sender, destinations, id, num, message, off, len);
        } 
        
        Message tmp = cache;
        cache = cache.next;
        tmp.next = null;
        size--;
        
        tmp.sender = sender;
        tmp.destinations = destinations;
        tmp.id = id;
        tmp.num = num;
        tmp.buffer = message;
        tmp.off = off;
        tmp.len = len;
        
        return tmp;        
    }
    
    public synchronized Message get(int len, int dst) { 
        
        if (size == 0 || len > MAX_MESSAGE_SIZE) { 
            
            if (len <= MAX_MESSAGE_SIZE) {
        //        System.err.println("Creating new message of size " + 
        //                MAX_MESSAGE_SIZE + " " + size);
                return new Message(MAX_MESSAGE_SIZE, dst);
            } else {
                System.err.println("Creating new message of size " + 
                        len + " " + size);

                return new Message(len, dst);
            }
        }
        
        Message tmp = cache;
        cache = cache.next;
        tmp.next = null;
        size--;
                
        return tmp;        
    }
    
    /*
    
    public synchronized Message get() {
        
        if (size == 0) { 
            return new Message();
        } 
        
        Message tmp = cache;
        cache = cache.next;
        tmp.next = null;
        size--;
        
        return tmp;
    } 
    */        
}
