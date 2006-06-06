package mcast.lrm;

//import ibis.ipl.IbisIdentifier;

public interface MessageReceiver {
    public boolean gotMessage(String sender, Message buffer);    
}
