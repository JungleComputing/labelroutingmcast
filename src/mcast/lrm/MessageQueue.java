package mcast.lrm;

import java.util.LinkedList;

public class MessageQueue {

    private LinkedList queue = new LinkedList();
    
    public synchronized void enqueue(Message m) { 
        
        if (m.destinations == null || m.destinations.length == 0) { 
            return; 
        }
                
        queue.addLast(m);
        notifyAll();
    }
    
    public synchronized Message dequeue() { 

        while (queue.size() == 0) { 
            try { 
                wait();
            } catch (Exception e) {
                // TODO: handle exception
            }
        }
        
        return (Message) queue.removeFirst();
    }
}
