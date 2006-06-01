package mcast.lrm;

//import ibis.ipl.IbisIdentifier;

public interface ByteArrayReceiver {
    public boolean gotMessage(String sender, int id, byte [] message);    
}
