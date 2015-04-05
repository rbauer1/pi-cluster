#include <stdio.h>
#include <qlibc/qlibc.h> //used for qhashtbl_t
#include <qlibc/containers/qhashtbl.h> //used for qhashtbl_t
#include <mpi.h> //used for mpi calls
#include <string.h> //used for strcpy
#include <unistd.h> //used for unzip() (I think.)
#include <dirent.h> //used for counting files
#include <strings.h> //for strcasecmp()
#include <stdlib.h> //for qsort() (and more?)
#include <mcheck.h> //TODO REMOVE THIS. for mtrace(), muntrace()

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

int convertFile2Plaintext(MPI_Datatype tally_t, char *buffer, int buffer_size, int world_rank);

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

int unzip(char *path){

    pid_t childpid;

    childpid = fork();

    if (childpid == -1){
        printf("Error in fork. Aborting.\n");
        return -1;
    }

    if (childpid == 0){
        //Child will handle unzipping the file
        printf("NOTE!!! if extracted files already exist here, this will NOT overwrite them!!!\n");
        execl("/usr/bin/unzip", "unzip", "-n", path, "OPS/main*", (char*) NULL);
    }else{
        //parent waits for child to finish before continuing
        wait(NULL);
        printf("Finished unzipping\n");
        return 0;
    }
    return 0;
}

//---------------------------------------------------------------------------

void * mergeTallies(int world_rank, Tally *tal, int n_tal, Tally *oth_tal, int n_oth_tal, Tally
*total_tal, int old_size, int *out_tal_len){
        double REALLOC_SCALE = 2;
        int tal_p = 0, oth_tal_p = 0, unique_entries = 0;

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
        strcpy(total_tal[0].word, "??????");

        int cmp;
/*        if(n_tal == 1){
            printf("%d tal1: %d tal2:%d tal[0]%s\n", world_rank, n_tal, n_oth_tal, tal[0].word);
        }else if(n_oth_tal == 1){
            printf("%d tal1: %d tal2:%d oth_tal[0]%s\n", world_rank, n_tal, n_oth_tal, oth_tal[0].word);
        }else{
            printf("%d tal1: %d tal2:%d\n", world_rank, n_tal, n_oth_tal);
        }
*/
        //merge tallies
        while(tal_p + oth_tal_p < sum){
//        while(tal_p + oth_tal_p < old_size){
            /*this if-else prevents the case where one array is finished before
             * the other, and an overflow occurs that corrupts the data. this
             * way, if one finishes before the other, the other simply fills in
             * the rest of the combined array */
            if(tal_p >= n_tal){
                cmp = 1;
            }else if(oth_tal_p >= n_oth_tal){
                cmp = -1;
            }else{
                cmp = strcasecmp(tal[tal_p].word, oth_tal[oth_tal_p].word);
            }
//            if(world_rank==4) printf("--%d--%d-%s:%d %d-%s:%d-------- diff:%d last_word:%s\n",world_rank, tal_p, tal[tal_p].word, tal[tal_p].num, oth_tal_p, oth_tal[oth_tal_p].word, oth_tal[oth_tal_p].num, cmp, last_word);
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
//                printf("%d tal1: %d tal2:%d\n", world_rank,  n_tal, n_oth_tal);
//                printf("%d RESCALING FROM %d to %d\n", world_rank, old_size, (int)(old_size*REALLOC_SCALE));
                old_size = (int)(REALLOC_SCALE * old_size);
                //need to allocate more memory for total_tal
//                printf("%d %p\n", world_rank, total_tal);
                Tally *tmp = realloc(total_tal, old_size * sizeof(Tally));
                if (tmp != NULL){
                    total_tal = tmp;
                }else{
                    printf("mem REallocation failed, aborting.\n");
                    return NULL;
                }
//                printf("%d %p\n", world_rank, total_tal);
//                printf("%d unique:%d tal_p%d oth_tal_p%d\n", world_rank, unique_entries, tal_p, oth_tal_p);
            }

        }

        /* PUT total_tal INTO FILE */
/*        FILE *out_file = fopen("words.txt", "w");

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
/*
        if(unique_entries > old_size){
            printf("-----");
        }
         printf("tal1: %d tal2:%d sum%d unique: %d\n", n_tal, n_oth_tal,sum, unique_entries);
*/
        //free(tal)
        *out_tal_len = unique_entries;
        /******free(total_tal); TODO MAKE SURE THIS IS TAKEN CARE OF SOMEWHERE ***/
        /** TODO Do the tally arrays that were passed in need to be freed? **/
        return total_tal;
}

//---------------------------------------------------------------------------

int readAndSendFileLoop(MPI_Datatype tally_t, int n_files, int world_rank, int world_size){
    const int MAX_LINE_LEN = 256;
    const int MAX_LINES = 4096;
    double time_s, time_e;
    int i;
    /* TODO mtrace says that there is a leak here of size 0x100000 which is the
     * size of file_buffer. however, it actually says the leak happens on the
     * snprintf line below. I'm not entirely sure what to make of it or do
     * about it currently */
    char *file_buffer = (char*)malloc(sizeof(char)*MAX_LINE_LEN*MAX_LINES);
    for(i = world_rank; i<n_files; i+=4){

        int n_lines = 0;
        /*  TODO
            NOTE: this doesn't actually need to be an array.
            it is helpful because it allows each line to be accessed if need be
            but it would function just as well for normal running as a single
            int. Once everything else works, this should be changed to a plain
            int to avoid wasting a moderate amount of memory needlessly.

            NOTE TOO: if this is changed from an array to a plain int, it needs
            to be set = 0 at the beginning of iteration of the for-loop
        */
        int line_start_index[MAX_LINES];

        //length of filename (should allow for 9999 pages)
        char file_name[17];

        //makes file_name into equivalent of "OPS/main"+i+".xml"
        snprintf(file_name, sizeof(file_name), "%s%03d", "OPS/main",i);

        FILE *file_reader = fopen(file_name, "rt");

        if (file_reader == NULL){
            printf("Error opening %s Aborting.\n", file_name);
            //fclose(out_file);
            return -1;
        }

        char line[MAX_LINE_LEN];
        while (fgets(line, MAX_LINE_LEN, file_reader) != NULL){
            strcpy(file_buffer + line_start_index[n_lines], line);
            int len = strlen(line);
            n_lines++;
            line_start_index[n_lines] = line_start_index[n_lines-1] + len + 1;
        }

        //printf("send buffer size = %d\n", line_start_index[n_lines]);
        if((i+4)%world_size != world_rank){
            time_s = MPI_Wtime();
            MPI_Send(file_buffer, line_start_index[n_lines], MPI_CHAR,
            (i+4)%world_size, world_rank, MPI_COMM_WORLD);
            time_e = MPI_Wtime();
//            printf("Time to send (file %d), size %dB, to (proc %d) from (proc %d) is %f\n", i, line_start_index[n_lines], (i+4)%16, world_rank, time_e-time_s);
        }else{
            convertFile2Plaintext(tally_t, file_buffer, line_start_index[n_lines], world_rank);
        }
        //clean up
        fclose(file_reader);
    }

    free(file_buffer);
    file_buffer = NULL;

    return 0;
}

//---------------------------------------------------------------------------

int countFiles(char *path){
    int n_files = 0;
    DIR *dir;
    struct dirent *entry;

    dir = opendir(path);
    if (dir == NULL){
        printf("Error opening directory");
        return -1;
    }
    while ((entry = readdir(dir)) != NULL){
        if (entry->d_type == DT_REG){
            n_files++;
        }
    }
    closedir(dir);
    return n_files;
}

//---------------------------------------------------------------------------

int findSplitLocation(Tally *tal, int length, char c){
    int i;
    for(i = 0; i < length; i++){
        if(tal[i].word[0] == c || tolower(tal[i].word[0]) == c){
            return i;
        }
    }
    return (length > 0) ? length-1 : 0;
}

//---------------------------------------------------------------------------

int beginMergeProcess(int world_rank, MPI_Datatype tally_t, Tally *tallies, int n_tallies){
    /* these strings are the characters that the tallies are split at at each
     * level. eg. at level 2 (0-indexed), the alphabet gets split w, k, q, e.
     *       as in: abcd Efghij Klmnop Qrstuv Wxyz
     * the strange order that the characters are in in the strings was chosen
     * so that each process could find the character it needed at the level it
     * needed it with the least amount of work. The map is just
     *      world_rank % (2^(level)) */
    char *split_chars[4] = {
        "k",
        "qf",
        "thnc",
        "uipdsgmb"
        };


    MPI_Status status;
    int i, n_inc_tallies = 0, split_loc, world_size;

    // get world_size
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    for(i = 0; (1 << i) < world_size && (1 << i) > 0; i++){
//    for(i = 0; i < 1; i++){
        MPI_Barrier(MPI_COMM_WORLD);

        /********************************************
        if( i == 8){
            int x = 0;
            printf("PID %d ready for attach\n", getpid());
            fflush(stdout);
            while (0 == x)
                sleep(20);
        }
        *******************************************/



        Tally *tp = tallies;

        split_loc = findSplitLocation(tallies, n_tallies, split_chars[i][world_rank % (1 << i)]);
        tp += split_loc;



        if ( ((world_rank >> i)&1) == 0 ){

            int tag = world_rank + MERGE_TAG + i*world_size;

//            printf("P%d is at merge-level %d send with tag: %d size %d\n", world_rank, i,tag, split_loc);

            MPI_Send(tallies, split_loc, tally_t, (1 << i) + world_rank, tag, MPI_COMM_WORLD);

            tag += (1 << i);


            //check how large incoming Tally array is
            MPI_Probe((1 << i) + world_rank, tag, MPI_COMM_WORLD, &status);
            MPI_Get_count(&status, tally_t, &n_inc_tallies);

            Tally inc_tallies[n_inc_tallies];
//            printf("P%d is at merge-level %d recv with tag: %d size %d\n", world_rank, i,tag,n_inc_tallies);

            MPI_Recv(inc_tallies, n_inc_tallies, tally_t, (1 << i) + world_rank, tag, MPI_COMM_WORLD, &status);

            int larger = (n_tallies - split_loc > n_inc_tallies) ? n_tallies - split_loc : n_inc_tallies;

//            printf("%d level %d larger: %d\n", world_rank, i, larger);
            Tally *total_tal = calloc(larger, sizeof(Tally));
            if ( total_tal == NULL){
                printf("mem allocation failed for total_tal (left), aborting.\n");
                //TODO add error handling
                return -1;
            }

//            printf("Proc %d is at merge-level %d l merge s\n", world_rank, i);
            total_tal = mergeTallies(world_rank, tp, n_tallies - split_loc, inc_tallies, n_inc_tallies, total_tal, larger, &n_tallies);
//            printf("%d %p TT:%p\n", world_rank, tallies, total_tal);
            free(tallies);
            tallies = NULL;
            tallies = total_tal;
//            printf("Proc %d is at merge-level %d l merge f\n", world_rank, i);
//            printf("%d %p\n", world_rank, tallies);
        }else{
            int tag = world_rank - (1 << i) + MERGE_TAG + i*world_size;


            //check how large incoming Tally array is
            MPI_Probe(world_rank - (1 << i), tag, MPI_COMM_WORLD, &status);
            MPI_Get_count(&status, tally_t, &n_inc_tallies);

            Tally inc_tallies[n_inc_tallies];
//            printf("P%d is at merge-level %d recv with tag: %d size %d\n", world_rank, i,tag,n_inc_tallies);

            MPI_Recv(inc_tallies, n_inc_tallies, tally_t, world_rank - (1 << i), tag, MPI_COMM_WORLD, &status);

            tag += (1 << i);

//            printf("P%d is at merge-level %d send with tag: %d size %d\n", world_rank, i,tag, n_tallies - split_loc);

            MPI_Send(tp, n_tallies - split_loc, tally_t, world_rank - (1 << i), tag, MPI_COMM_WORLD);

            int larger = (split_loc > n_inc_tallies) ? split_loc : n_inc_tallies;
//            printf("%d level: %d larger: %d\n", world_rank, i, larger);
            Tally *total_tal = calloc(larger, sizeof(Tally));
            if ( total_tal == NULL){
                printf("mem allocation failed for total_tal (right), aborting.\n");
                //TODO add error handling
                return -1;
            }

//            printf("Proc %d is at merge-level %d r merge s\n", world_rank, i);
            total_tal =mergeTallies(world_rank, tallies, split_loc, inc_tallies, n_inc_tallies, total_tal, larger, &n_tallies);
//            printf("%d %p\n", world_rank, tallies);
//            printf("%d %p TT:%p\n", world_rank, tallies, total_tal);
            free(tallies);
            tallies = NULL;
            tallies = total_tal;
//            printf("%d %p\n", world_rank, tallies);
//            printf("Proc %d is at merge-level %d r merge f\n", world_rank, i);
        }
//        MPI_Barrier(MPI_COMM_WORLD);
    }
    char file_name[12];

    snprintf(file_name, sizeof(file_name), "words%d-%d.x", i, world_rank);

    FILE *out_file = fopen(file_name, "w");

    if (out_file == NULL){
        printf("Error opening out_file in beginMergeProcess(). Aborting.\n");
        return -1;
    }
    for(i = 0; i < n_tallies; i++){
        fprintf(out_file, "%s : %d\n", tallies[i].word, tallies[i].num);
    }

    //clean up
    fclose(out_file);

    free(tallies);
    tallies = NULL;

    //TODO not doing anything currently
    return 0;
}
//---------------------------------------------------------------------------

/**TODO This is terrible and a quick and inadequate fix for the real problem of
 * encoding. These books are encoded in UTF-8 and as such can't simply be
 * parsed as ascii files. this is causing numerous problems down the execution,
 * which is why for now I am just going to apply this dirty "fix" so that when
 * other things are working I can come back and actually deal with it
int isAscii */

//---------------------------------------------------------------------------

int convertFile2Plaintext(MPI_Datatype tally_t, char *buffer, int buffer_size, int world_rank){
    double time_s, time_e;
    time_s = MPI_Wtime();

//    printf("%d is convertingFile2Plaintext!\n",world_rank);

    //create hashtable
    qhashtbl_t *hash = qhashtbl(1000,0);

    //this will hold each word
    char word[ WORD_LEN ];

    int i, word_index;

    word_index = 0;

    //might need to add 1 to buffer_size to deal with terminator at end of
    //string. make sure to check after it's working

    for(i = 0; i < buffer_size; i++){
        if (buffer[i] == '<'){
            while (i < buffer_size && buffer[i] != '>'){
                i++;
            }
        }else{
            switch(buffer[i]){
                //these cases are possible word delineators (not exhaustive)
            case '\n':
            case ' ':
            case '\t':
            case '\0':
            case ',':
            case '.':
            case ':':
            case ';':
            case '?':
            case '!':
            case '(':
            case ')':
            case '"':
                if (word != NULL && word_index != 0){
                    word[word_index] = '\0';
                    int n = hash->getint(hash, word);
                    hash->putint(hash, word, ++n);
                    word_index = 0;
                }
                break;
            default:
                word[word_index] = buffer[i];
                word_index++;
            }
        }
    }

    /* this next section can and probably should be a separate method */

    int n_tallies = hash->size(hash);

    Tally *tallies = malloc(sizeof(Tally) * n_tallies);

    qhashtbl_obj_t obj;
    //must be cleared before call
    memset((void*) &obj, 0, sizeof(obj));
    for (i = 0; hash->getnext(hash, &obj, true); i++){
        //dump hash keys:values into Tally array
        strcpy( tallies[i].word, obj.name);
        tallies[i].num = atoi((char*)obj.data); //ew. improve if possible
//        if(world_rank == 4) printf("%s\n", tallies[i].word);
        free(obj.name);
        free(obj.data);
    }

    hash->free(hash);

    //sort this process's Tally array
    //TODO this is very likely a bottleneck
    qsort(tallies, n_tallies, sizeof(Tally), tallyCmp);

    time_e = MPI_Wtime();

//    printf("(proc %d) time to convert and sort file: %fs\n", world_rank, time_e-time_s);

//    printf("%d is beginning merge\n", world_rank);
    beginMergeProcess(world_rank, tally_t, tallies, n_tallies);

   //TODO not doing anything currently
    return 0;
}

//---------------------------------------------------------------------------

void recvFileLoop(MPI_Datatype tally_t, int world_rank, int world_size){
    int buffer_size, old_buffer_size = -1, i, n_files;
    MPI_Status status;
    char *incoming_file;

    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Bcast(&n_files, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Barrier(MPI_COMM_WORLD);

    for(i = world_rank; i < n_files; i+=world_size){

        //find out how big file_buffer is using this and next call
        MPI_Probe(world_rank%4, world_rank%4, MPI_COMM_WORLD, &status);

        //get file_buffer size and store in buffer_size
        MPI_Get_count(&status, MPI_CHAR, &buffer_size);

        //printf("recv buffer_size = %d\n", buffer_size);

        if (buffer_size < 0){
            printf("Error: buffer_size = %d\n", buffer_size);
            //TODO add some kind of error handling
            return;
        }

        if (old_buffer_size == -1){
            incoming_file = (char*)malloc(sizeof(char) * buffer_size);
        }else if (buffer_size > old_buffer_size){
            //double the requested size. might be too much
            incoming_file = (char*)realloc(incoming_file, buffer_size*2);
        }

        if (incoming_file == NULL){
            //TODO add error handling
            return;
        }


        MPI_Recv(incoming_file, buffer_size, MPI_CHAR, world_rank%4, world_rank%4, MPI_COMM_WORLD, &status);

//      incoming_file[25] = '\0';

//      MPI_Get_count(&status, MPI_CHAR, &buffer_size);

//      printf("pi: %d msg_len: %d msg: %s tally_t: %d\n", world_rank, buffer_size, incoming_file, &tally_t);

        convertFile2Plaintext(tally_t, incoming_file, buffer_size, world_rank);

        old_buffer_size = buffer_size;
    }

    free(incoming_file);
    incoming_file = NULL;

}

//---------------------------------------------------------------------------

int main(int argc, char* argv[]){

    /* TODO there are multiple other functions that return -1 on errors. These
     * need to actually be handled in the case of that they return -1, as
     * currently, the functions that call them don't actually check their
     * returns*/
    if (argc < 2){
        printf("Usage: run [path/to/epub]\n");
        return -1;
    }


    int world_rank, world_size, name_length, n_files;
    char hostname[MPI_MAX_PROCESSOR_NAME];

    MPI_Init(NULL, NULL);

    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Get_processor_name(hostname, &name_length);

//    if(world_rank == 0) mtrace(); //TODO REMOVE
    /* Check if mpi world time clocks are synchronized.
     * 1 if they are, 0 if they aren't.
     * (they are not for this system)
    void *global;
    int f;
    MPI_Attr_get(MPI_COMM_WORLD, MPI_WTIME_IS_GLOBAL, &global, &f);
    printf("%d %d ---  %p synchronized? %d\n", world_rank, MPI_WTIME_IS_GLOBAL, global, *((int*)global));
    */

    double start_time, end_time;
    //use barrier to sync times to degree possible (?)
    //MPI_Barrier(MPI_COMM_WORLD);
    start_time = MPI_Wtime();

    printf("Hello from %s, processor %d, process: %ld\n", hostname, world_rank, (long)getpid());

    //setup mpi derived type "Tally" used to transmit individual results
    MPI_Datatype tally_t;
    initializeTallyType(&tally_t);

    if (world_rank < 4){ //procs 0-4 (those on pi01) deal with distributing the files

        //process 0 unzips file
/*        if(world_rank == 0){
            if (unzip(argv[1]) == -1){
                printf("Unzipping failed. Aborting.\n");
                return -1;
            }
            n_files = countFiles("./OPS/");
            printf("Number of files: %d\n", n_files);
        }
*/
        //TODO REMOVE!!!!
        n_files = 16;

        MPI_Barrier(MPI_COMM_WORLD);
        MPI_Bcast(&n_files, 1, MPI_INT, 0, MPI_COMM_WORLD);
        MPI_Barrier(MPI_COMM_WORLD);

        readAndSendFileLoop(tally_t, n_files, world_rank, world_size);

        //is this needed? (another below)
        //MPI_Barrier(MPI_COMM_WORLD);

    }else{
        recvFileLoop(tally_t, world_rank, world_size);
    }


    MPI_Type_free(&tally_t);

    //use barrier to sync times to degree possible (?)
    //MPI_Barrier(MPI_COMM_WORLD);

    end_time = MPI_Wtime();

    printf("-------------------------Total time (proc %d): %f\n", world_rank, end_time - start_time);

    MPI_Barrier(MPI_COMM_WORLD);

    MPI_Finalize();

//    if(world_rank == 0) muntrace(); //TODO REMOVE
    return 0;
}

