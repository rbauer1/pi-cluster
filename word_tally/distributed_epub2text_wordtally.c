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

}

//---------------------------------------------------------------------------

void mergeTallies(Tally *tal, int n_tal, Tally *oth_tal, int n_oth_tal){

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

        /*this is used to catch cases where the next word being placed into
         * total_tal is the same as the last word that was placed there except
         * it has a different case. this happens whenever one list has the
         * upper and lower case of a word, and the other only has only upper or
         * lower. the checks for this add a lot of comparisons, so if it is
         * possible to get around this somehow, it's probably worth it*/
        char last_word[ WORD_LEN ];
        //something that is not a word
        strcpy(last_word, "!!!!!!");

        //merge tallies
        while(tal_p + oth_tal_p < sum){
            int cmp = strcasecmp(tal[tal_p].word, oth_tal[oth_tal_p].word);
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
        FILE *out_file = fopen("words.txt", "w");

        if (out_file == NULL){
            printf("Error opening out_file. Aborting.\n");
            return;
        }

        int i;
        for(i = 0; i < unique_entries; i++){
            fprintf(out_file, "%s : %d\n", total_tal[i].word, total_tal[i].num);
        }

        fclose(out_file);

        //printf("FINAL TALLY\n");
        //printTally(total_tal, unique_entries);
        //printf("\n");
        free(total_tal);
}

//---------------------------------------------------------------------------

int readAndSendFileLoop(int n_files, int world_size){
    const int MAX_LINE_LEN = 256;
    const int MAX_LINES = 4096;
    double time_s, time_e;
    int i;
    char *file_buffer = (char*)malloc(sizeof(char)*MAX_LINE_LEN*MAX_LINES);
    for(i = 0; i<n_files; i++){

        int n_lines = 0;
        /*  NOTE: this doesn't actually need to be an array.
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
        snprintf(file_name, sizeof(file_name), "%s%d%s", "OPS/main",i,".xml\0");

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
        time_s = MPI_Wtime();
        MPI_Send(file_buffer, line_start_index[n_lines], MPI_CHAR, (i%(world_size-1))+1, 0, MPI_COMM_WORLD);
        time_e = MPI_Wtime();
        printf("Time to send (file %d), size %dB, to (proc %d) is %f\n", i, line_start_index[n_lines], (i%(world_size-1))+1, time_e-time_s);

        //clean up
        fclose(file_reader);
    }

    free(file_buffer);

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

int convertFile2Plaintext(MPI_Datatype tally_t, char *buffer, int buffer_size, int world_rank){
    double time_s, time_e;
    time_s = MPI_Wtime();

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

    int num_tallies = hash->size(hash);

    Tally tally[num_tallies];

    qhashtbl_obj_t obj;
    //must be cleared before call
    memset((void*) &obj, 0, sizeof(obj));
    for (i = 0; hash->getnext(hash, &obj, true); i++){
        //dump hash keys:values into Tally array
        strcpy( tally[i].word, obj.name);
        tally[i].num = atoi((char*)obj.data); //ew. improve if possible
        free(obj.name);
        free(obj.data);
    }
    time_e = MPI_Wtime();
    printf("(proc %d) time to convert file: %fs\n", world_rank, time_e-time_s);

    hash->free(hash);

    //sort this process's Tally array
    time_e = MPI_Wtime();
    qsort(tally, num_tallies, sizeof(Tally), tallyCmp);
    time_e = MPI_Wtime();


    printf("(proc %d) time to sort file: %fs\n", world_rank, time_e-time_s);

    if ( world_rank % 2 == 0 ){

        MPI_Send(tally, num_tallies, tally_t, world_rank+1, world_rank, MPI_COMM_WORLD);
                                //******************************************************************************************
                                //******************************************************************************************
                                //******************************************************************************************
                                //******************************************************************************************
    } else if( world_rank != 1){ //THIS IS TEMPORARY. SOMETHING NEEDS TO BE DONE WITH PROCESSOR 1!!!
                                //******************************************************************************************
                                //******************************************************************************************
                                //******************************************************************************************
                                //******************************************************************************************

        int n_inc_tallies = 0;
        MPI_Status status;

        //check how large the incoming Tally array is
        MPI_Probe(world_rank-1, world_rank-1, MPI_COMM_WORLD, &status);
        MPI_Get_count(&status, tally_t, &n_inc_tallies);
        Tally inc_tallies[n_inc_tallies];
        MPI_Recv(inc_tallies, n_inc_tallies, tally_t, world_rank-1, world_rank-1, MPI_COMM_WORLD, &status);
        /* next 5 lines are a check, might not be necessary */
        int n_recvd_tallies;
        MPI_Get_count(&status, tally_t, &n_recvd_tallies);
        if(n_recvd_tallies != n_inc_tallies){
            printf("ERROR: %d did not recieve full message from %d\n", world_rank, world_rank - 1);
            printf("Wanted %d, got %d\n", n_inc_tallies, n_recvd_tallies);
            return -1;
        }
        time_s = MPI_Wtime();
        mergeTallies(tally, num_tallies, inc_tallies, n_inc_tallies);
        time_e = MPI_Wtime();

        printf("(proc %d) time to merge files: %fs\n", world_rank, time_e-time_s);

    }

    //not doing anything currently
    return 0;
}

//---------------------------------------------------------------------------

void workerLoop(MPI_Datatype tally_t, int world_rank, int world_size){
    int buffer_size, old_buffer_size = -1;
    MPI_Status status;
    char *incoming_file;

    //find out how big file_buffer is using this and next call
    MPI_Probe(0,0, MPI_COMM_WORLD, &status);

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


    MPI_Recv(incoming_file, buffer_size, MPI_CHAR, 0, 0, MPI_COMM_WORLD, &status);

    //incoming_file[25] = '\0';

    //MPI_Get_count(&status, MPI_CHAR, &buffer_size);

    //printf("pi: %d msg_len: %d msg: %s tally_t: %d\n", world_rank, buffer_size, incoming_file, &tally_t);

    convertFile2Plaintext(tally_t, incoming_file, buffer_size, world_rank);

    old_buffer_size = buffer_size;

    free(incoming_file);

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


    int world_rank, world_size, name_length;
    char hostname[MPI_MAX_PROCESSOR_NAME];

    MPI_Init(NULL, NULL);

    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Get_processor_name(hostname, &name_length);

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

    if (world_rank == 0){
        if (unzip(argv[1]) == -1){
            printf("Unzipping failed. Aborting.\n");
            return -1;
        }

        int n_files = countFiles("./OPS/");
        printf("Number of files: %d\n", n_files);

        readAndSendFileLoop(3, world_size);

        //is this needed? (another below)
        //MPI_Barrier(MPI_COMM_WORLD);

    }else{
        workerLoop(tally_t, world_rank, world_size);
    }

    MPI_Type_free(&tally_t);

    //use barrier to sync times to degree possible (?)
    //MPI_Barrier(MPI_COMM_WORLD);

    end_time = MPI_Wtime();

    printf("Total time (proc %d): %f\n", world_rank, end_time - start_time);

    MPI_Finalize();

    return 0;
}



