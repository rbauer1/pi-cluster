/* With a lot of help from www.linux-mag.com/id/1332/ */

#include <mpi.h>
#include <stdio.h>
#include <string.h> //for strcpy()
#include <strings.h> //for strcasecmp()
#include <stdlib.h> //for qsort()

#include <qlibc/qlibc.h>
#include <qlibc/containers/qhashtbl.h>

#define WORD_LEN 20
const int TALLIES = 8;


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

int tallyCmp(const void *a , const void*b){
    return strcasecmp((*(Tally*)a).word, (*(Tally*)b).word);
}

void mergeTallies(MPI_Datatype tally_type){
        //these could be more flexibly allocated using mpi-probe in a real
        //application
        Tally tal[TALLIES], other_tal[TALLIES];

        //receive now sorted arrays
        MPI_Recv(tal, TALLIES, tally_type, 1, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        MPI_Recv(other_tal, TALLIES, tally_type, 2, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        //start at size of 1.5 (don't know how many overlaps there will be)
        Tally *total_tal = malloc(((int)(TALLIES*1.5)) * sizeof(Tally));

        if ( total_tal == NULL){
            printf("mem allocation failed, aborting.\n");
            return;
        }

        //old_size should be whatever int total_tal was malloc-ed with (in this
        //case (int)(1.5*TALLIES))
        int tal_p = 0, oth_tal_p = 0, unique_entries = 0, old_size = TALLIES;

        while(tal_p + oth_tal_p < (TALLIES * 2) - 1){
            int cmp = strcasecmp(tal[tal_p].word, other_tal[oth_tal_p].word);
            if (cmp == 0){
                strcpy(total_tal[unique_entries].word, tal[tal_p].word);
                total_tal[unique_entries].num =
                        tal[tal_p].num + other_tal[oth_tal_p].num;
                unique_entries++;
                tal_p++;
                oth_tal_p++;
            }else if (cmp < 0){
                strcpy(total_tal[unique_entries].word, tal[tal_p].word);
                total_tal[unique_entries].num = tal[tal_p].num;
                unique_entries++;
                tal_p++;
            }else{
                strcpy(total_tal[unique_entries].word, other_tal[oth_tal_p].word);
                total_tal[unique_entries].num = other_tal[oth_tal_p].num;
                unique_entries++;
                oth_tal_p++;
            }


            if (unique_entries == old_size){
                //need to allocate more memory for total_tal
                total_tal = realloc(total_tal, ((int)(old_size*1.5)) * sizeof(Tally));
                if ( total_tal == NULL){
                    printf("mem REallocation failed, aborting.\n");
                    return;
                }
                old_size = (int)(old_size*1.5);
            }

        }
        printf("FINAL TALLY\n");
        printTally(total_tal, unique_entries);
        printf("\n");
        free(total_tal);
}

int main(void){
    MPI_Init(NULL, NULL);

    int world_rank, world_size;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

    Tally tal[TALLIES];
    /*i: used for for-loop
    * base: used to convert absolute addresses to relative (see loop below)
    * blocklen: holds number of a given type of data in the datatype. ie, there
    *  are WORD_LEN chars, and 1 int in a Tally. */
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

    /*this makes offset point to the *relative* address instead of the absolute
    * one. ie: before loop:
    *   offset[0] = base = X
    *   offset[1] = X + Y
    * after loop:
    *   offset[0] = 0
    *   offset[1] = Y */
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
        char *words[] = {"apple", "apricot", "banana", "blackberry", "cherry", "date", "elderberry", "fig"};
        int vals[] = {1,3,5,7,9,11,13,15};
        Tally other_tal[TALLIES];
        for ( i = 0; i < TALLIES; i++){
            strcpy( other_tal[i].word, words[i] );
            other_tal[i].num = vals[i];
        }

        strcpy( tal[0].word, "date");
        tal[0].num = 1;
        strcpy( tal[1].word, "fig");
        tal[1].num = 3;
        strcpy( tal[2].word, "cherry");
        tal[2].num = 5;
        strcpy( tal[3].word, "elderberry");
        tal[3].num = 7;
        strcpy( tal[4].word, "apple");
        tal[4].num = 9;
        strcpy( tal[5].word, "banana");
        tal[5].num = 11;
        strcpy( tal[6].word, "blackberry");
        tal[6].num = 13;
        strcpy( tal[7].word, "apricot");
        tal[7].num = 15;

        printf("Sending:\n");
        printTally(tal, TALLIES);
        printf("\n");
        MPI_Send(tal, TALLIES, tally_type, 1, 0, MPI_COMM_WORLD);

        printf("Sending:\n");
        printTally(other_tal, TALLIES);
        printf("\n");
        MPI_Send(other_tal, TALLIES, tally_type, 2, 0, MPI_COMM_WORLD);

        mergeTallies(tally_type);
    }else{
        //receive the Tally array and print contents for verification
        MPI_Recv(tal, TALLIES, tally_type, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        printf("Recieved:\n");
        printTally(tal, TALLIES);
        printf("\n");

        //sort this process's Tally array
        qsort(tal, TALLIES, sizeof(Tally), tallyCmp);

        //send sorted array back to root
        MPI_Send(tal, TALLIES, tally_type, 0, 1, MPI_COMM_WORLD);
    }

    MPI_Finalize();
    return 0;
}
