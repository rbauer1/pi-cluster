#include <stdio.h>
#include <qlibc/qlibc.h> //used for qtreetbl_t
#include <qlibc/containers/qtreetbl.h> //used for qtreetbl_t
#include <mpi.h> //used for mpi calls
#include <string.h> //used for strcpy
#include <stdlib.h> //for qsort() and malloc etc

/* technically longest commonly accepted word is 45 letters, however, for
 * everything to include marry poppins we'd use 34. this number will likely
 * have a large impact on memory usage, so a more realistic number like 15 or
 * 20 is probably sufficient. Note that the struct will be padded to the
 * nearest 4 bytes. This means that since num is an int, so 4 bytes, using
 * anything number that is not a multiple of 4 for WORD_LEN is wasting space*/
#define WORD_LEN 24

static const int LINE_LEN = 80;

static const int MERGE_TAG = 9;

static double sum_par_merge_time = 0;
static int n_par_merge = 0;

static double sum_parse_tally_time = 0;
static int n_parse_tally = 0;

static double sum_read_send_time = 0;
static int n_read_send = 0;

typedef struct{
    char word[ WORD_LEN ];
    int num;
} Tally;

int parseAndTally(MPI_Datatype tally_t, char *buffer, int buffer_size, int world_rank, int iteration_count);

//---------------------------------------------------------------------------

double reduceTime(double time_s, double time_e, int world_rank, int world_size, char* mesg, int print){

    double total_time = time_e - time_s, average_time = 0;

    MPI_Reduce(&total_time, &average_time, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);

    average_time = average_time / world_size;

//  printf("-----Total time (proc %d): %f\n", world_rank, end_time - start_time);

    if(world_rank==0 && print == 1) printf("%s%f\n", mesg, average_time);
    return average_time;

}

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
//debugging
void * mergeTallies(Tally *tal, int n_tal, Tally *oth_tal, int n_oth_tal, Tally *total_tal, int old_size, int *out_tal_len){
       double REALLOC_SCALE = 2;
        int tal_p = 0, oth_tal_p = 0, unique_entries = 0;

        //total number of words (loop-guard)
        int sum = n_tal + n_oth_tal;
        int cmp;
        //merge tallies
        while(tal_p + oth_tal_p < sum){
            /*this if-else prevents the case where one array is finished before
             * the other, and an overflow occurs that corrupts the data. this
             * way, if one finishes before the other, the other simply fills in
             * the rest of the combined array */
            if(tal_p >= n_tal){
                cmp = 1;
            }else if(oth_tal_p >= n_oth_tal){
                cmp = -1;
            }else{
                cmp = strcmp(tal[tal_p].word, oth_tal[oth_tal_p].word);
            }
            if (cmp == 0){
                    strcpy(total_tal[unique_entries].word, tal[tal_p].word);
                    total_tal[unique_entries].num = tal[tal_p].num + oth_tal[oth_tal_p].num;
                    unique_entries++;
                tal_p++;
                oth_tal_p++;
            }else if (cmp < 0){
                    strcpy(total_tal[unique_entries].word, tal[tal_p].word);
                    total_tal[unique_entries].num = tal[tal_p].num;
                    unique_entries++;
                tal_p++;
            }else{
                    strcpy(total_tal[unique_entries].word, oth_tal[oth_tal_p].word);
                    total_tal[unique_entries].num = oth_tal[oth_tal_p].num;
                    unique_entries++;
                oth_tal_p++;
            }


            if (unique_entries == old_size){
                old_size = (int)(REALLOC_SCALE * old_size);
                //need to allocate more memory for total_tal
                Tally *tmp = realloc(total_tal, old_size * sizeof(Tally));
                if (tmp != NULL){
                    total_tal = tmp;
                }else{
                    printf("mem REallocation failed, aborting.\n");
                    return NULL;
                }
            }

        }

        *out_tal_len = unique_entries;
        return total_tal;
}

//---------------------------------------------------------------------------

int readAndSendFileLoop(MPI_Datatype tally_t, int n_files, int world_rank, int world_size){
    const int MAX_LINE_LEN = 128;
    const int MAX_LINES = 2048;
    double time_s, time_e;
    int i, count = 0;
    /* TODO mtrace says that there is a leak here of size 0x100000 which is the
     * size of file_buffer. however, it actually says the leak happens on the
     * snprintf line below. I'm not entirely sure what to make of it or do
     * about it currently */
    char *file_buffer = (char*)malloc(sizeof(char)*MAX_LINE_LEN*MAX_LINES);
    for(i = world_rank; i<n_files; i+=4){
        int line_start_index = 0;
         if((i+4)%world_size != world_rank) time_s = MPI_Wtime();

        //length of filename (should allow for 9999 pages)
        char file_name[17];

        //makes file_name into equivalent of "OPS/main"+i+".xml"
        snprintf(file_name, sizeof(file_name), "OPS/main%03d", i);

        FILE *file_reader = fopen(file_name, "rt");

        if (file_reader == NULL){
            printf("Error opening %s Aborting.\n", file_name);
            //fclose(out_file);
            return -1;
        }

        char line[MAX_LINE_LEN];
        while (fgets(line, MAX_LINE_LEN, file_reader) != NULL){
            strcpy(file_buffer + line_start_index, line);
            int len = strlen(line);
            line_start_index += len + 1;
        }

        //printf("send buffer size = %d\n", line_start_index[n_lines]);
        if((i+4)%world_size != world_rank){
            MPI_Send(file_buffer, line_start_index, MPI_CHAR, (i+4)%world_size, world_rank, MPI_COMM_WORLD);
            time_e = MPI_Wtime();
            sum_read_send_time += time_e - time_s;
            n_read_send++;
//            printf("Time to send (file %d), size %dB, to (proc %d) from (proc %d) is %f\n", i, line_start_index[n_lines], (i+4)%16, world_rank, time_e-time_s);
        }else{
            parseAndTally(tally_t, file_buffer, line_start_index, world_rank, count);
            count++;
        }
        //clean up
        fclose(file_reader);
    }

    free(file_buffer);
    file_buffer = NULL;

    return count;
}

//---------------------------------------------------------------------------

int findSplitLocation(Tally *tal, int length, char c){
    int i;
    for(i = 0; i < length; i++){
        if(tal[i].word[0] == c){
            return i;
        }
    }
    return (length > 0) ? length-1 : 0;
}

//---------------------------------------------------------------------------

int parallelMerge(int world_rank, MPI_Datatype tally_t, Tally *tallies, int n_tallies, int iteration_count){
    /* these strings are the characters that the tallies are split at at each
     * level. eg. at level 2 (0-indexed), the alphabet gets split t, h, n, c.
     *       resulting in: ab Cde fg Hij klm Nop qrs Tuvwxyz
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
        MPI_Barrier(MPI_COMM_WORLD);

        Tally *tp = tallies;

        split_loc = findSplitLocation(tallies, n_tallies, split_chars[i][world_rank % (1 << i)]);
        tp += split_loc;



        if ( ((world_rank >> i)&1) == 0 ){

            int tag = world_rank + MERGE_TAG + i*world_size;


            MPI_Send(tallies, split_loc, tally_t, (1 << i) + world_rank, tag, MPI_COMM_WORLD);

            tag += (1 << i);


            //check how large incoming Tally array is
            MPI_Probe((1 << i) + world_rank, tag, MPI_COMM_WORLD, &status);
            MPI_Get_count(&status, tally_t, &n_inc_tallies);

            Tally inc_tallies[n_inc_tallies];

            MPI_Recv(inc_tallies, n_inc_tallies, tally_t, (1 << i) + world_rank, tag, MPI_COMM_WORLD, &status);

            int larger = (n_tallies - split_loc > n_inc_tallies) ? n_tallies - split_loc : n_inc_tallies;

            Tally *total_tal = calloc(larger, sizeof(Tally));
            if ( total_tal == NULL){
                printf("mem allocation failed for total_tal (left), aborting.\n");
                //TODO add error handling
                return -1;
            }

            total_tal = mergeTallies(tp, n_tallies - split_loc, inc_tallies, n_inc_tallies, total_tal, larger, &n_tallies);
            free(tallies);
            tallies = NULL;
            tallies = total_tal;
        }else{
            int tag = world_rank - (1 << i) + MERGE_TAG + i*world_size;


            //check how large incoming Tally array is
            MPI_Probe(world_rank - (1 << i), tag, MPI_COMM_WORLD, &status);
            MPI_Get_count(&status, tally_t, &n_inc_tallies);

            Tally inc_tallies[n_inc_tallies];

            MPI_Recv(inc_tallies, n_inc_tallies, tally_t, world_rank - (1 << i), tag, MPI_COMM_WORLD, &status);

            tag += (1 << i);


            MPI_Send(tp, n_tallies - split_loc, tally_t, world_rank - (1 << i), tag, MPI_COMM_WORLD);

            int larger = (split_loc > n_inc_tallies) ? split_loc : n_inc_tallies;
            Tally *total_tal = calloc(larger, sizeof(Tally));
            if ( total_tal == NULL){
                printf("mem allocation failed for total_tal (right), aborting.\n");
                //TODO add error handling
                return -1;
            }

            total_tal =mergeTallies(tallies, split_loc, inc_tallies, n_inc_tallies, total_tal, larger, &n_tallies);
            free(tallies);
            tallies = NULL;
            tallies = total_tal;
        }
    }
    char file_name[20];

    snprintf(file_name, sizeof(file_name), "words%d-%d.x", iteration_count, world_rank);

    FILE *out_file = fopen(file_name, "w");

    if (out_file == NULL){
        printf("Error opening out_file in parallelMerge(). Aborting.\n");
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

//---------------------------------------------------------------------------
/**TODO This function does not account for alternate encodings besides
 * ascii! in limited proof-of-concept examples this is okay, but it is a
 * serious problem if it is ever expected to handle other encodings (such as
 * utf-8) */
int parseAndTally(MPI_Datatype tally_t, char *buffer, int buffer_size,
int world_rank, int iteration_count){
    double time_s, time_e;
    int world_size;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    time_s = MPI_Wtime();

    //create left-leaning rb tree
    //can also use param QTREETBL_THREADSAFE for obvious purpose
    qtreetbl_t *tree = qtreetbl(0);

    //this will hold each word
    char word[ WORD_LEN ];

    int i, word_index;

    word_index = 0;

    //might need to add 1 to buffer_size to deal with terminator at end of
    //string. make sure to check after it's working
    /*TODO this function doesn't know how to handle ' since one could write
     * 'don't' in which parsing the word is much more complicated than the
     * simple cases that are usually encountered */
    for(i = 0; i < buffer_size; i++){
        if (buffer[i] == '<'){
            while (i < buffer_size && buffer[i] != '>'){
                i++;
            }
        }else{
            switch(buffer[i]){
                //these cases are possible word delineators (not exhaustive)
            case '\r':
                i++;
            case '-':
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
                    size_t s;
                    int *count = (int*)tree->get(tree, word, &s, false);
                    if (count == NULL){
                        int c = 1;
                        tree->put(tree, word, &c, sizeof(int));
                    }else{
                        *count += 1;
                    }
                    word_index = 0;
                }
                break;
            default:
                /*TODO 'tolower' eliminates the need to deal with
                 * capitalization differences but might be slower always
                 * calling it like this than just dealing with it at the end of
                 * execution when there are significantly fewer words to check.
                 * however! if it is not done here, then the tree will need to
                 * be passed a comparison function that compares strings
                 * ignoring cases, otherwise it will cause segfaults.
                 *
                 * NOTE: if tolower is not called before parallelMerge(),
                 * then findSplitLocation() will need to be updated to account
                 * for the starting characters possibly being of different
                 * cases. this WILL break the progam if not heeded.*/
                word[word_index] = tolower(buffer[i]);
                word_index++;
            }
        }
    }

    /* this next section can and probably should be a separate method */

    int n_tallies = tree->size(tree);
    Tally *tallies = calloc(n_tallies, sizeof(Tally));
    qtreetbl_obj_t tree_obj;
    memset((void*)&tree_obj, 0, sizeof(tree_obj)); //must be cleared before call
    for (i = 0; tree->getnext(tree, &tree_obj, false); i++){
        strcpy( tallies[i].word, tree_obj.name);
        int * c = (int*)tree_obj.data;
        tallies[i].num = *c;
    }

    tree->free(tree);

    time_e = MPI_Wtime();

    sum_parse_tally_time += reduceTime(time_s, time_e, world_rank, world_size, "parseAndTally Time:", 0);
    n_parse_tally++;


    time_s = MPI_Wtime();

    parallelMerge(world_rank, tally_t, tallies, n_tallies, iteration_count);

    time_e = MPI_Wtime();

    sum_par_merge_time += reduceTime(time_s, time_e, world_rank, world_size, "Parallel Merge Time:", 0);
    n_par_merge++;

   //TODO not doing anything currently
    return 0;
}

//---------------------------------------------------------------------------

int recvFileLoop(MPI_Datatype tally_t, int world_rank, int world_size){
    int buffer_size, old_buffer_size = -1, i, n_files, count = 0;
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

        parseAndTally(tally_t, incoming_file, buffer_size, world_rank, count);
        count++;

        old_buffer_size = buffer_size;
    }

    free(incoming_file);
    incoming_file = NULL;
    return count;
}

//---------------------------------------------------------------------------

int mergeLocal(int world_rank, int n_iterations, int level, MPI_Datatype tally_t){
    const int MAX_LINES = 4096;
    const int MAX_LINE_LEN = 64;
    const int FILE_NAME_LEN = 20;
    double time_s, time_e;
    int i,j,k;
    char word[ WORD_LEN ];
    char suffix[level+1];
    for(i = 0; i < level; i++){
        suffix[i]='x';
    }
    suffix[i] = '\0';
    Tally tally1[MAX_LINES];
    Tally tally2[MAX_LINES];
    char num[12]; //max digits for count of number
    int n_tal[2] = {0,0};
    /** if there are an odd number of files at this level, this int allows the
     * last one to simply be renamed instead of read and immediately printed
     * back out */
    int adj_iterations = n_iterations - (n_iterations % 2);
    for(i = 0; i < adj_iterations; i++){

        //length of filename (should allow for 9999 pages)
        char file_name[FILE_NAME_LEN];

        snprintf(file_name, sizeof(file_name) * FILE_NAME_LEN, "words%d-%d%s.x",i,world_rank,suffix);

//        if(world_rank==0) printf("i: %d, level: %d READING file: %s\n", i, level, file_name);
        FILE *file_reader = fopen(file_name, "rt");

        if (file_reader == NULL){
            printf("rank: %d Error opening %s Aborting.\n", world_rank, file_name);
            return -1;
        }

        char line[MAX_LINE_LEN];
        while (fgets(line, MAX_LINE_LEN, file_reader) != NULL){
            for(j = 0; j < MAX_LINE_LEN; j++){
                if(line[j] != ' ' && line[j] != '\0'){
                    word[j] = line[j];
                }else{
                    word[j] = '\0';
                    break;
                }
            }
            /* each line is <word> : <num> this moves the pointer to the first
             * digit of the number */
            j+= 3;

            for(k=0; line[j] != '\0' && k < 12; k++){
                num[k] = line[j];
                j++;
            }
            num[k] = '\0';
            if (word[0] != '\0'){
                if (i % 2 == 0){
                    strcpy(tally1[n_tal[0]].word, word);
                    tally1[n_tal[0]].num = atoi(num);
                    n_tal[0]++;
                }else{
                    strcpy(tally2[n_tal[1]].word, word);
                    tally2[n_tal[1]].num = atoi(num);
                    n_tal[1]++;
                }
            }
        }

        if (i % 2 == 1){
            int n_total_tal = (n_tal[0] > n_tal[1]) ? n_tal[0] : n_tal[1];
            Tally *total_tal = calloc(n_total_tal, sizeof(Tally));
            if ( total_tal == NULL){
                printf("mem allocation failed for total_tal (left), aborting.\n");
                //TODO add error handling
                return -1;
            }


            //merge
            total_tal = mergeTallies(tally1, n_tal[0], tally2, n_tal[1], total_tal, n_total_tal, &n_total_tal);

            if(n_iterations > 2){
                 //write out file
                char file[FILE_NAME_LEN];
                snprintf(file, sizeof(file) * FILE_NAME_LEN, "words%d-%d%sx.x", i/2, world_rank, suffix);

//                if(world_rank==0) printf("i: %d, level: %d writing file: %s\n", i, level, file);
                FILE *out_file = fopen(file, "w");

                if (out_file == NULL){
                    //TODO add error handling!
                    printf("Error opening out_file in mergeLocal(). Aborting.\n");
                    return -1;
                }

                for(k = 0; k < n_total_tal; k++){
                    fprintf(out_file, "%s : %d\n", total_tal[k].word, total_tal[k].num);
                }
                fclose(out_file);

            }else if (world_rank > 3){

                MPI_Send(total_tal, n_total_tal, tally_t, world_rank%4, world_rank, MPI_COMM_WORLD);

            }else{
                //this rank's file goes last, so its tally array needs to be
                //stored while the other rank's data is received and written
                Tally *tmp_tally = calloc(n_total_tal, sizeof(Tally));
                tmp_tally = memcpy(tmp_tally, total_tal, n_total_tal * sizeof(Tally));
                //write out file
                char file[FILE_NAME_LEN];
                int l, tmp_total = n_total_tal;
                MPI_Status status;
                int order[4] = {12,4,8,0};

                snprintf(file, sizeof(file) * FILE_NAME_LEN, "words%d.x", world_rank);

                FILE *out_file = fopen(file, "w");

                if (out_file == NULL){
                    //TODO add error handling!
                    printf("Error opening out_file in mergeLocal(). Aborting.\n");
                    return -1;
                }

                for(l = 0; l < 4; l++){
                    int current = world_rank + order[l];

                    if(l != 3){
                        MPI_Probe(current, current, MPI_COMM_WORLD, &status);
                        MPI_Get_count(&status, tally_t, &n_total_tal);
                        free(total_tal);
                        total_tal = calloc(n_total_tal, sizeof(Tally));
                        if (total_tal == NULL){
                            printf("%d calloc failed in mergeLocal from %d\n",world_rank, current);
                            return -1;
                        }
                        MPI_Recv(total_tal, n_total_tal, tally_t, current, current, MPI_COMM_WORLD, &status);
                    }else{
                        //restore this rank's data to finally be written
                        free(total_tal);
                        total_tal = tmp_tally;
                        n_total_tal = tmp_total;
                    }
/* not needed if tolower called in parseAndTally()
                    int m, running_total;
                    for(k = 0; k < n_total_tal; k++){
                        running_total = total_tal[k].num;
                        for(m = 1; m + k < n_total_tal; m++){
                            if(strcmp(total_tal[k+m].word, total_tal[k].word) == 0){
                                running_total += total_tal[k+m].num;
                            }else{
                                m--;
                                break;
                            }
                        }
                        //don't have this function but might be a good idea to do
                        //strlwr(total_tal[k].word);
                        fprintf(out_file, "%s : %d\n", total_tal[k].word, running_total);
                        k+=m;
                    }
*/
                    for(k = 0; k< n_total_tal; k++){
                        fprintf(out_file, "%s : %d\n", total_tal[k].word, total_tal[k].num);
                    }
                }
                fclose(out_file);
            }

            //clean up
            free(total_tal);
            total_tal = NULL;
            n_tal[0] = 0;
            n_tal[1] = 0;
        }
        //clean up
        fclose(file_reader);
        int delete_file_status = remove(file_name);
        if(delete_file_status != 0){
            //TODO add error handling
            printf("Error: failed trying to delete file: %s Aborting.\n", file_name);
            return -1;
        }

    }
    if (n_iterations % 2 == 1 && n_iterations > 2){
        char new_name[FILE_NAME_LEN], old_name[FILE_NAME_LEN];
        snprintf(new_name, sizeof(new_name) * FILE_NAME_LEN, "words%d-%d%sx.x",n_iterations/2,world_rank,suffix);
        snprintf(old_name, sizeof(old_name) * FILE_NAME_LEN, "words%d-%d%s.x",n_iterations-1,world_rank,suffix);
        int rename_file_status = rename(old_name, new_name);
        if(rename_file_status != 0){
            //TODO add error handling
            printf("Renaming a file failed in mergeLocal(). old_name:%s new_name:%s Aborting\n",old_name,new_name);
            return -1;
        }
    }
    if(n_iterations > 1){
        mergeLocal(world_rank, (n_iterations/2) + (n_iterations%2), level+1, tally_t);
    }
    return 0;
}
//---------------------------------------------------------------------------

int finalConcat(){
    int i;
    const int MAX_LINE_LEN = 64;
    char file[19] = "allWordsParallel.x\0";

    FILE *out_file = fopen(file, "w");

    if (out_file == NULL){
        //TODO add error handling!
        printf("Error opening out_file in finalConcat(). Aborting.\n");
        return -1;
    }

    for(i = 3; i > -1; i--){
        //length of filename (should allow for 9999 pages)
        char file_name[10];

        //makes file_name into equivalent of "OPS/main"+i+".xml"
        snprintf(file_name, sizeof(file_name), "words%d.x", i);

        FILE *file_reader = fopen(file_name, "rt");

        if (file_reader == NULL){
            printf("Error opening %s in finalConcat(). Aborting.\n", file_name);
            //fclose(out_file);
            return -1;
        }

        char line[MAX_LINE_LEN];
        while (fgets(line, MAX_LINE_LEN, file_reader) != NULL){
            fputs(line, out_file);
        }
        //clean up
        fclose(file_reader);
    }

    //clean up
    fclose(out_file);

    return 0;
}

//---------------------------------------------------------------------------

int main(int argc, char* argv[]){

    /* TODO there are multiple other functions that return -1 on errors. These
     * need to actually be handled in the case of that they return -1, as
     * currently, the functions that call them don't actually check their
     * returns*/
    /*if (argc < 2){
        printf("Usage: run [path/to/epub]\n");
        return -1;
    }*/

    if (argc < 2){
        printf("Usage: run [num_files]\n");
        return -1;
    }

    int world_rank, world_size, name_length, n_files, n_iterations;
//    char hostname[MPI_MAX_PROCESSOR_NAME];

    //TODO currently segfaults if n_files%16 != 0
    n_files = atoi(argv[1]);
    if (n_files%16 != 0){
        printf("Error. Currently only works with the number of files being a multiple of 16. Aborting.\n");
        return -1;
    }


    MPI_Init(NULL, NULL);

    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    double start_time, end_time;
    start_time = MPI_Wtime();

//    printf("Hello from %s, processor %d, process: %ld\n", hostname, world_rank, (long)getpid());

    //setup mpi derived type "Tally" used to transmit individual results
    MPI_Datatype tally_t;
    initializeTallyType(&tally_t);

    if (world_rank < 4){ //procs 0-4 (those on pi01) deal with distributing the files

        /* TODO this broadcast is used because the eventual goal is to have the
         * number of files be counted automatically, meaning only procs 0-4
         * will actually be able to find that number and so a broadcast will be
         * needed. For now it is supplied by command args, but this is intended
         * to be temporary. */
        MPI_Barrier(MPI_COMM_WORLD);
        MPI_Bcast(&n_files, 1, MPI_INT, 0, MPI_COMM_WORLD);
        MPI_Barrier(MPI_COMM_WORLD);

        n_iterations = readAndSendFileLoop(tally_t, n_files, world_rank, world_size);

        //is this needed? (another below)
        //MPI_Barrier(MPI_COMM_WORLD);

    }else{
        n_iterations = recvFileLoop(tally_t, world_rank, world_size);
    }

    double time_s, time_e;
    time_s = MPI_Wtime();
    mergeLocal(world_rank, n_iterations, 0, tally_t);
    time_e = MPI_Wtime();
    reduceTime(time_s, time_e, world_rank, world_size, "Merge Local Time:", 1);


    MPI_Type_free(&tally_t);

    /*This call prevents a race condition wherein 0 starts concatenating the
     * final files before (one or more of) 1-3 have finished printing their
     * file, thereby leaving out that quarter of the final text. There may be
     * another way to get around this, but for now this line is critical.*/
    MPI_Barrier(MPI_COMM_WORLD);

    if(world_rank==0){
        double t_s, t_e;
        t_s = MPI_Wtime();
        finalConcat();
        t_e = MPI_Wtime();
        printf("Final Concat Time:%f\n",t_e-t_s);//Final Concat Time
    }


    end_time = MPI_Wtime();

    reduceTime(start_time, end_time, world_rank, world_size, "Total Time:", 1);

    if(world_rank > 0 && world_rank < 4){
        MPI_Send(&sum_read_send_time, 1, MPI_DOUBLE, 0, 10, MPI_COMM_WORLD);
    }

    if(world_rank==0){
        double proc1_rs_time, proc2_rs_time, proc3_rs_time;
        MPI_Recv(&proc1_rs_time, 1, MPI_DOUBLE, 1, 10, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        MPI_Recv(&proc2_rs_time, 1, MPI_DOUBLE, 2, 10, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        MPI_Recv(&proc3_rs_time, 1, MPI_DOUBLE, 3, 10, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        //the two lines below mimic the behavior of the reduce used for the other timings
        sum_read_send_time += proc1_rs_time + proc2_rs_time + proc3_rs_time;
        sum_read_send_time /= 4;
        printf("Total Read and Send File Time:%f\n", sum_read_send_time);
        printf("Avg Read and Send File Time:%f: %d\n", sum_read_send_time/n_read_send, n_read_send);
        printf("Total Parallel Merge Time:%f\n", sum_par_merge_time);
        printf("Avg Parallel Merge Time:%f: %d\n", sum_par_merge_time/n_par_merge, n_par_merge);
        printf("Total Parse & Tally Time:%f\n", sum_parse_tally_time);
        printf("Avg Parse & Tally Time:%f: %d\n", sum_parse_tally_time/n_parse_tally, n_parse_tally);
    }

    MPI_Barrier(MPI_COMM_WORLD);

    MPI_Finalize();

    return 0;
}

