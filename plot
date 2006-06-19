set terminal postscript
set xlabel "Machines"
set ylabel "Throughput (MByte/sec)"
plot "results-simple-new-16k.txt" with linespoints title "New 16K", "results-simple-new-32k.txt" with linespoints title "New 32K", "results-simple-new-8k.txt" with linespoints title "New 8K", "results-simple.txt" with linespoints title "Old" 
