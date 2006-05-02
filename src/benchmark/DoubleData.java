package benchmark;

import java.io.Serializable;

public class DoubleData implements Serializable {

    private static final long serialVersionUID = -5344957666572055285L;

    int iteration;
    double[] accs_x;
    double[] accs_y;
    double[] accs_z;
    
    DoubleData(int size) { 
        
        accs_x = new double[size];
        accs_y = new double[size];
        accs_z = new double[size];
        
        iteration = 0;
        
        for (int i=0;i<size;i++) { 
            accs_x[i] = Math.random();
            accs_y[i] = Math.random();
            accs_z[i] = Math.random();
        }
    }
    
    int getSize() {         
        return 4 + 8*accs_x.length + 8*accs_y.length + 8*accs_z.length;        
    }
}
