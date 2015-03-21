#include <mpi.h>
#include <stdio.h>
#include <string.h>


typedef struct{
    char word[100][20];
    int num[100];
} count;

int main(void){
    MPI_Init(NULL, NULL);

    int world_rank, world_size;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

    struct count d;
    char *loc;

    MPI_Datatype count_type, old_types[2];



    if (world_rank == 0){
        strcpy( d.word[0], "word0");
        d.num[0] = 1;
        strcpy( d.word[1], "word1");
        d.num[1] = 2;
        strcpy( d.word[2], "word2");
        d.num[2] = 4;
        strcpy( d.word[3], "word3");
        d.num[3] = 8;
        strcpy( d.word[4], "word4");
        d.num[4] = 16;
        strcpy( d.word[5], "word5");
        d.num[5] = 32;

    }else{
        MPI_Recv(
    }

    MPI_Finalize();
    return 0;
}
