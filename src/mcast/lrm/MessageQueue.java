package mcast.lrm;

public class MessageQueue {

    private final int limit;
    
    private Message head; 
    private Message tail; 
    
    private int size = 0;

    public MessageQueue() { 
        // no limit...
        this(Integer.MAX_VALUE);
    }
    
    public MessageQueue(int limit) { 
        this.limit = limit;
    }
    
    public synchronized int size() { 
        return size;
    }

    /**
     * Wait for a while. Return false if interrupted.
     */
    private synchronized boolean doWait() {
        try {
            wait(2000);
        } catch(InterruptedException e) {
            // Someone wants us to stop ...
            return false;
        }
        if (Thread.currentThread().interrupted()) {
            // Someone wants us to stop ...
            return false;
        }
        return true;
    }
    
    public synchronized boolean enqueue(Message m) { 

        while (size >= limit) {            
            if (! doWait()) {
                // Someone wants us to stop ...
                return false;
            }
        }

        if (head == null) { 
            head = tail = m;
            m.next = null;
            notifyAll();
        } else { 
            tail.next = m;
            tail = m;            
        }
        
        size++;
        return true;
    }
    
    public synchronized Message dequeue() { 

        while (size == 0) { 
            if (! doWait()) {
                // Someone wants us to stop ...
                return null;
            }
        }

        Message tmp = head;         
        head = head.next;
        tmp.next = null;
        
        size--;

        if (size == limit-1) { 
            notifyAll();
        }
        
        return tmp;
    }
}
