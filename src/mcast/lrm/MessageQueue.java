package mcast.lrm;

import java.util.LinkedList;

public class MessageQueue {

    private LinkedList queue = new LinkedList();    
    private int limit;
    
    public MessageQueue(int limit) { 
        this.limit = limit;
    }
    
    public synchronized void enqueue(Message m) { 
        
        while (queue.size() >= limit) {            
            try { 
               System.err.println("Send queue hit limit! (" + limit + ")");
                wait();
            } catch (Exception e) {
                // TODO: handle exception
            }
        }
        
        queue.addLast(m);
        
        if (queue.size() == 1) { 
            notifyAll();
        } 
    }
    
    public synchronized Message dequeue() { 

        while (queue.size() == 0) { 
            try { 
                wait();
            } catch (Exception e) {
                // TODO: handle exception
            }
        }
        
        if (queue.size() == limit) { 
            notifyAll();
        } 
        return (Message) queue.removeFirst();
    }
}
