#include <stdio.h>
#include <qlibc/qlibc.h> //used for qhashtbl_t
#include <qlibc/containers/qhashtbl.h> //used for qhashtbl_t
#include <mpi.h> //used for mpi calls
#include <string.h> //used for strcpy
#include <unistd.h> //used for unzip() (I think.)
#include <dirent.h> //used for counting files
#include <strings.h> //for strcasecmp()
#include <stdlib.h> //for qsort() (and more?)

/* technically longest commonly accepted word is 45 letters, however, for
 * everything to include marry poppins we'd use 34. this number will likely
 * have a large impact on memory usage, so a more realistic number like 15 or
 * 20 is probably sufficient. Note that the struct will be padded to the
 * nearest 4 bytes. This means that since num is an int, so 4 bytes, using
 * anything number that is not a multiple of 4 for WORD_LEN is wasting space*/
#define WORD_LEN 24

static const int LINE_LEN = 80;

static const int MERGE_TAG = 9;

typedef struct{
    char word[ WORD_LEN ];
    int num;
} Tally;

//---------------------------------------------------------------------------

int tallyCmp(const void *a , const void*b){
    return strcasecmp((*(Tally*)a).word, (*(Tally*)b).word);
}

//---------------------------------------------------------------------------


/*Used simply to verify successful send/recv */
void printTally(Tally *t, int num_tally){
    int i;
    printf("%d tally object(s)\n", num_tally);
    for(i = 0; i < num_tally; i++){
        printf("%s : %d\n", t[i].word, t[i].num);
    }
}

//---------------------------------------------------------------------------

/* With a lot of help from www.linux-mag.com/id/1332/ */

void initializeTallyType(MPI_Datatype *tally_type){

    /*i: used for for-loop
    * base: used to convert absolute addresses to relative (see loop below)
    * blocklen: holds number of a given type of data in the datatype. ie, there
    *  are WORD_LEN chars, and 1 int in a Tally. */
    int i, base, blocklen[] = { WORD_LEN , 1};

    Tally tally;
    strcpy( tally.word, "test");
    tally.num = 1;

    //the new mpi-derived datatype
//    MPI_Datatype tally_type;
    //the primitive mpi-types that a tally_type is made from
    MPI_Datatype types[] = {MPI_CHAR, MPI_INT};
    // mpi-address-integers to delineate the derived datatype. 2 because there
    // are two primitive types composing the new datatype
    MPI_Aint offset[2];

    //offset[0] now points to start of struct d
    //and by extension, start of d.word
    MPI_Address(&tally, offset);
    //offset[1] now points to start of d.num
    MPI_Address(&tally.num, offset+1);

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
    MPI_Type_struct(2, blocklen, offset, types, tally_type);

    //commit the new datatype so that it may then be used
    MPI_Type_commit(tally_type);

}

//---------------------------------------------------------------------------

Tally * mergeTallies(Tally *tal, int n_tal, Tally *oth_tal, int n_oth_tal, int
world_rank, int *out_tal_len){

        int larger = (n_tal > n_oth_tal) ? n_tal : n_oth_tal;
        //start at size of 1.5 (don't know how many overlaps there will be)
        Tally *total_tal = malloc(((int)(larger*1.5)) * sizeof(Tally));

        if ( total_tal == NULL){
            printf("mem allocation failed for total_tal, aborting.\n");
            return;
        }

        //old_size should be whatever int total_tal was malloc-ed with (in this
        //case (int)(1.5*max(n_tal, n_oth_tal))
        int tal_p = 0, oth_tal_p = 0, unique_entries = 0, old_size = (int)(1.5*larger);

        //total number of words (loop-guard)
        int sum = n_tal + n_oth_tal;

        /* TODO: this can be likely be done one time per processor at the very
         * end of the program's execution. this will slightly increase the size
         * of the files being thrown around, but it should be a pretty
         * significant speedup since this doubles the number of comparisons.
         *
         * this is used to catch cases where the next word being placed into
         * total_tal is the same as the last word that was placed there except
         * it has a different case. this happens whenever one list has the
         * upper and lower case of a word, and the other only has only upper or
         * lower. the checks for this add a lot of comparisons, so if it is
         * possible to get around this somehow, it's probably worth it*/
        char last_word[ WORD_LEN ];
        //something that is not a word
        strcpy(last_word, "!!!!!!");

        int cmp;
        //merge tallies
        while(tal_p + oth_tal_p < sum){
            if(tal_p >= n_tal){
                cmp = 1;
            }else if (oth_tal_p >= n_oth_tal){
                cmp = -1;
            }else{
                cmp = strcasecmp(tal[tal_p].word, oth_tal[oth_tal_p].word);
            }
            //printf("------%s:%d  %s:%d-------- diff:%d last_word:%s\n",tal[tal_p].word, tal[tal_p].num, oth_tal[oth_tal_p].word, oth_tal[oth_tal_p].num, cmp, last_word);
            if (cmp == 0){
                if (strcasecmp(tal[tal_p].word, last_word) == 0){
                    total_tal[unique_entries-1].num += tal[tal_p].num + oth_tal[oth_tal_p].num;
                }else{
                    strcpy(total_tal[unique_entries].word, tal[tal_p].word);
                    total_tal[unique_entries].num =
                            tal[tal_p].num + oth_tal[oth_tal_p].num;
                    unique_entries++;
                }
                tal_p++;
                oth_tal_p++;
            }else if (cmp < 0){
                if (strcasecmp(tal[tal_p].word, last_word) == 0){
                    total_tal[unique_entries-1].num += tal[tal_p].num;
                }else{
                    strcpy(total_tal[unique_entries].word, tal[tal_p].word);
                    total_tal[unique_entries].num = tal[tal_p].num;
                    unique_entries++;
                }
//                printf("ding\n");
                tal_p++;
            }else{
                if (strcasecmp(oth_tal[oth_tal_p].word, last_word) == 0){
                    total_tal[unique_entries-1].num += oth_tal[oth_tal_p].num;
                }else{
                    strcpy(total_tal[unique_entries].word, oth_tal[oth_tal_p].word);
                    total_tal[unique_entries].num = oth_tal[oth_tal_p].num;
                    unique_entries++;
                }
                oth_tal_p++;
            }

            //update last_word
            strcpy(last_word, total_tal[unique_entries-1].word);

           // printf("%s:%d\n", total_tal[unique_entries-1].word, total_tal[unique_entries-1].num);

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

        /* PUT total_tal INTO FILE */
/*        //11 to allow for words10.txt etc in future
        char file_name[12];

        snprintf(file_name, sizeof(file_name), "%s%d%s","words",world_rank,".txt\0");

        FILE *out_file = fopen(file_name, "w");

        if (out_file == NULL){
            printf("Error opening out_file. Aborting.\n");
            return;
        }

        int i;
        for(i = 0; i < unique_entries; i++){
            fprintf(out_file, "%s : %d\n", total_tal[i].word, total_tal[i].num);
        }

        fclose(out_file);
*/

        //printf("FINAL TALLY\n");
        //printTally(total_tal, unique_entries);
        //printf("\n");

        *out_tal_len = unique_entries;

        /****free(total_tal); make sure this is taken care of somewhere***/
        /**TODO do i need to free the tally arrays that were passed in?**/
        return total_tal;
}

//---------------------------------------------------------------------------

int findSplitLocation(Tally *tal, int length, char c){
    int i;
    for(i = 0; i < length; i++){
        //NOTE: this does NOT account for CAPS!!!
        if (tal[i].word[0] >= c){
            return i;
        }
    }
    return (length > 0) ? length-1 : 0;
}


//---------------------------------------------------------------------------

int splitData(MPI_Datatype tally_t, int world_rank, int world_size){

    int i, num_tallies = 26;

    char *names1[] = {
        "amy",
        "barry",
        "catherine",
        "david",
        "elizabeth",
        "frank",
        "gail",
        "hank",
        "irena",
        "jeff",
        "kendra",
        "luke",
        "madeline",
        "nathan",
        "olivia",
        "paul",
        "quentin",
        "riley",
        "shannon",
        "tom",
        "uri",
        "vince",
        "wendy",
        "xavier",
        "yulia",
        "zack" };

    char *names2[] = {
        "aaron",
        "bridget",
        "chris",
        "dakota",
        "erik",
        "fay",
        "george",
        "helen",
        "isaac",
        "julia",
        "keefe",
        "lydia",
        "matt",
        "natalia",
        "oliver",
        "petra",
        "quincy",
        "ruth",
        "stan",
        "tina",
        "uther",
        "vivian",
        "walt",
        "xi",
        "yusef",
        "zephyr" };

    Tally tally1[num_tallies];
    Tally tally2[num_tallies];

    for(i = 0; i < num_tallies; i++){
        strcpy( tally1[i].word, names1[i]);
        tally1[i].num = i;
        strcpy( tally2[i].word, names2[i]);
        tally2[i].num = i;
    }


    MPI_Status status;
    int n_inc_tallies = 0, split_loc;
    Tally *out_tal;
    /*this should be unnecessary in real program */
    if( world_rank % 2 == 0){
        out_tal = tally1;
    }else{
        out_tal = tally2;
    }
    /* these strings are the characters that the tallies are split at at each
     * level. eg. at level 2 (0-indexed), the alphabet gets split w, k, q, e.
     *       as in: abcd Efghij Klmnop Qrstuv Wxyz
     * the strange order that the characters are in in the strings was chosen
     * so that each process could find the character it needed at the level it
     * needed it with the least amount of work. The map is just
     *      world_rank % (2^(level)) */
    char *split_chars[4] = {
            "n",
            "th",
            "wkqe",
            "ymsguioc"
        };


    for(i = 0; (1 << i) < world_size && (1 << i) > 0; i++){
        /*TODO this conditional (the ==0 part) is dumb, but functions fine and can be changed later */
        if ( ((world_rank >> i)&1) == 0 ){
            printf("i:%d rank: %d, sending right to %d, receiving from %d\n", i, world_rank, (1 << i)+world_rank, (1 << i)+world_rank);
            Tally *tp1 = out_tal;

            /* determine the location at which the tally array needs to be
             * split for this level in the merge */
            split_loc = findSplitLocation(out_tal, num_tallies,split_chars[i][world_rank%(1 << i)]);


            MPI_Send(out_tal, split_loc, tally_t, (1 << i)+world_rank,
            MERGE_TAG, MPI_COMM_WORLD);

            //check how large the incoming Tally array is
            MPI_Probe((1 << i) + world_rank, MERGE_TAG, MPI_COMM_WORLD, &status);
            MPI_Get_count(&status, tally_t, &n_inc_tallies);
//        printf("n_inc_tallies: %d\n", n_inc_tallies);
            Tally inc_tallies[n_inc_tallies];
            MPI_Recv(inc_tallies, n_inc_tallies, tally_t, (1 << i)+world_rank,
            MERGE_TAG, MPI_COMM_WORLD, &status);
            tp1 += split_loc;
//        printf("inc_tallies[num_tallies].word: %s\n", inc_tallies[n_inc_tallies-1].word);

            out_tal = mergeTallies(tp1, num_tallies-split_loc, inc_tallies,
            n_inc_tallies, world_rank, &num_tallies);

        } else{

            printf("i:%d rank: %d, sending left to %d, receiving from %d\n", i, world_rank, world_rank - (1 << i), world_rank - (1 << i));

            Tally *tp2 = out_tal;

            split_loc = findSplitLocation(out_tal, num_tallies,
            split_chars[i][world_rank%(1<<i)]);

            tp2 += split_loc;

//        printf("tp2[num_tallies/2].word: %s\n", (*(tp2+num_tallies/2 - 1)).word);

            //check how large the incoming Tally array is
            MPI_Probe(world_rank - (1 << i), MERGE_TAG, MPI_COMM_WORLD, &status);
            MPI_Get_count(&status, tally_t, &n_inc_tallies);
            Tally inc_tallies[n_inc_tallies];
            MPI_Recv(inc_tallies, n_inc_tallies, tally_t, world_rank-(1 << i),
            MERGE_TAG, MPI_COMM_WORLD, &status);

            MPI_Send(tp2, num_tallies-split_loc, tally_t, world_rank-(1 << i),
            MERGE_TAG, MPI_COMM_WORLD);

            /* next 5 lines are a check, might not be necessary */
            int n_recvd_tallies;
            MPI_Get_count(&status, tally_t, &n_recvd_tallies);
            if(n_recvd_tallies != n_inc_tallies){
                printf("ERROR: %d did not recieve full message from %d\n", world_rank, world_rank - 1);
                printf("Wanted %d, got %d\n", n_inc_tallies, n_recvd_tallies);
                return -1;
            }
            out_tal = mergeTallies(out_tal, split_loc, inc_tallies,
            n_inc_tallies, world_rank, &num_tallies);

        }
        //free(out_tal);
         /* PUT total_tal INTO FILE */
        //11 to allow for words10.txt etc in future
        char file_name[14];

        snprintf(file_name, sizeof(file_name), "%s%d-%d%s","words",i,world_rank,".txt\0");

        FILE *out_file = fopen(file_name, "w");

        if (out_file == NULL){
            printf("Error opening out_file. Aborting.\n");
            return;
        }
        int j;
        for(j = 0; j < num_tallies; j++){
            fprintf(out_file, "%s : %d\n", out_tal[j].word, out_tal[j].num);
        }

        fclose(out_file);

    }

    //not doing anything currently
    return 0;
}


//---------------------------------------------------------------------------

int main(int argc, char* argv[]){

    /* TODO there are multiple other functions that return -1 on errors. These
     * need to actually be handled in the case of that they return -1, as
     * currently, the functions that call them don't actually check their
     * returns*/


    int world_rank, world_size, name_length, n_files;
    char hostname[MPI_MAX_PROCESSOR_NAME];

    MPI_Init(NULL, NULL);

    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Get_processor_name(hostname, &name_length);


//    printf("Hello from %s, processor %d, process: %ld\n", hostname, world_rank, (long)getpid());

    //setup mpi derived type "Tally" used to transmit individual results
    MPI_Datatype tally_t;
    initializeTallyType(&tally_t);

    splitData(tally_t, world_rank, world_size);

    MPI_Type_free(&tally_t);


    MPI_Finalize();

    return 0;
}



