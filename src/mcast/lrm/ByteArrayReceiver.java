package mcast.lrm;

//import ibis.ipl.IbisIdentifier;

public interface ByteArrayReceiver {
    public boolean gotMessage(String sender, int id, int num, byte [] message);    
}
