package lrmcast;

import java.util.Comparator;
import java.util.Arrays;

import ibis.ipl.IbisIdentifier;

public class IbisSorter implements Comparator {

    // General sorter to use when no cluster order is preferred. 
    private static final IbisSorter sorter = new IbisSorter("unknown"); 
    
    private final String preferredCluster; 
           
    private IbisSorter(String preferredCluster) { 
        this.preferredCluster = preferredCluster;
    }

    public static void sort(IbisIdentifier [] ids) { 
        sort(ids, 0, ids.length);
    }    

    public static void sort(IbisIdentifier local, IbisIdentifier [] ids) { 
        sort(local, ids, 0, ids.length);
    }

    
    public static void sort(IbisIdentifier [] ids, int from, int to) { 
        Arrays.sort(ids, from, to, sorter);
    }    

    public static void sort(IbisIdentifier local, IbisIdentifier [] ids, 
            int from, int to) { 
    
        IbisSorter tmp = sorter;
        
        if (!local.cluster().equals(sorter.preferredCluster)) { 
            tmp = new IbisSorter(local.cluster());
        }
        
        Arrays.sort(ids, from, to, tmp);
    }

    public int compare(Object o1, Object o2) {

        IbisIdentifier id1 = (IbisIdentifier) o1;
        IbisIdentifier id2 = (IbisIdentifier) o2;
        
        String cluster1 = id1.cluster();
        String cluster2 = id2.cluster();
        
        if (cluster1.equals(cluster2)) { 
            // The clusters are identical, so the order depends on the names
            return id1.name().compareTo(id2.name()); 
        } 
        
        // The clusters are different. If one of the two is equal to the 
        // preferredCluster, we want that one to win. Otherwise, we just return 
        // the 'natural order'.  
        if (cluster1.equals(preferredCluster)) { 
            return -1;
        } 
        
        if (cluster2.equals(preferredCluster)) {  
            return 1;
        } 

        return cluster1.compareTo(cluster2);
    }    
}
