package mcast.lrm;

//import ibis.ipl.IbisIdentifier;

public interface MessageReceiver {
    // Returns false if the ObjectMulticaster is done().
    public boolean gotMessage(Message buffer);    
}
