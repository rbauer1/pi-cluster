#include <mpi.h>
#include <stdio.h>
#include <string.h>

#define WORD_LEN 20
const int TALLIES = 6;


typedef struct{
    char word[ WORD_LEN ];
    int num;
} Tally;

/* Used simply to verify successful send/recv */
void printTally(Tally *t, int num_tally){
    int i;
    printf("%d tally object(s)\n", num_tally);
    for(i = 0; i < num_tally; i++){
        printf("%s : %d\n", t[i].word, t[i].num);
    }
}


int main(void){
    MPI_Init(NULL, NULL);

    int world_rank, world_size;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

    Tally tal[TALLIES];
    //i: used for for-loop
    //base: used to convert absolute addresses to relative (see loop below)
    //blocklen: holds number of a given type of data in the datatype. ie, there
    //  are WORD_LEN chars, and 1 int in a Tally.
    int i, base, blocklen[] = {WORD_LEN, 1};

    //the new mpi-derived datatype
    MPI_Datatype tally_type;
    //the primitive mpi-types that a tally_type is made from
    MPI_Datatype types[] = {MPI_CHAR, MPI_INT};
    // mpi-address-integers to delineate the derived datatype. 2 because there
    // are two primitive types composing the new datatype
    MPI_Aint offset[2];

    //offset[0] now points to start of struct d
    //and by extension, start of d.word
    MPI_Address(tal, offset);
    //offset[1] now points to start of d.num
    MPI_Address(&tal[0].num, offset+1);

    //this makes offset point to the *relative* address instead of the absolute
    //one. ie: before loop:
    //  offset[0] = base = X
    //  offset[1] = X + Y
    //after loop:
    //  offset[0] = 0
    //  offset[1] = Y
    base = offset[0];
    for(i = 0; i < 2; i++){
        offset[i] -= base;
    }

    //'assemble' the new derived datatype
    MPI_Type_struct(2, blocklen, offset, types, &tally_type);

    //commit the new datatype so that it may then be used
    MPI_Type_commit(&tally_type);

    //populate the Tally array and send it
    if (world_rank == 0){

        strcpy( tal[0].word, "word0");
        tal[0].num = 1;
        strcpy( tal[1].word, "word1");
        tal[1].num = 2;
        strcpy( tal[2].word, "word2");
        tal[2].num = 4;
        strcpy( tal[3].word, "word3");
        tal[3].num = 8;
        strcpy( tal[4].word, "word4");
        tal[4].num = 16;
        strcpy( tal[5].word, "word5");
        tal[5].num = 32;

        printf("Sending:\n");
        printTally(tal, TALLIES);

        MPI_Send(tal, TALLIES, tally_type, 1, 0, MPI_COMM_WORLD);

    }else{
        //receive the Tally array and print contents for verification
        MPI_Recv(tal, TALLIES, tally_type, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        printf("Recieved:\n");
        printTally(tal, TALLIES);
    }

    MPI_Finalize();
    return 0;
}
