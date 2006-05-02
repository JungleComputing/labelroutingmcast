package lrmcast;

import ibis.ipl.IbisIdentifier;

public interface ByteArrayReceiver {
    public void gotMessage(IbisIdentifier sender, byte [] message);    
}
