#!/usr/bin/zsh

date +%M:%S
N=16
cd /home/pi/cluster/word_tally/OPS
rm main*
split -nl/$N -d count_of_monte_cristo.txt main -a 3
cd ..
for ((i = 0; i < 4; i++)); do
    echo "$N----------------------" >> times.txt;
    for ((j = 0; j < 100; j++)); do
        echo "$i $j"
        mpiexec -f /home/pi/machinefile -n 16 /home/pi/cluster/word_tally/tally.o $N >> times.txt;
    done
    let "N*=2"
    cd OPS;
    rm main*;
    split -nl/$N -d count_of_monte_cristo.txt main -a 3;
    cd ..;
done

date +%M:%S
