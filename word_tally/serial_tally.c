#include <stdio.h>
#include <time.h> //for timing
#include <qlibc/qlibc.h> //used for qhashtbl_t
#include <qlibc/containers/qhashtbl.h> //used for qhashtbl_t
#include <string.h> //used for strcpy
#include <unistd.h> //used for unzip() (I think.)
#include <dirent.h> //used for counting files
#include <strings.h> //for strcasecmp()
#include <stdlib.h> //for qsort() (and more?)
#include <math.h> //for floor TODO this is dumb. get around it

//#include <mcheck.h> //TODO REMOVE THIS. for mtrace(), muntrace()

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

int convertFile2Plaintext(char *buffer, int buffer_size, int iteration_count);

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


/**TODO This function does not account for alternate encodings besides
 * ascii!!!! in limited proof-of-concept examples this is okay, but it is a
 * serious problem if it is ever expected to handle other encodings (such as
 * utf-8) */

int readAndTally(int n_files){
    const int MAX_LINE_LEN = 128;
    int i, j;
    /* TODO mtrace says that there is a leak here of size 0x100000 which is the
     * size of file_buffer. however, it actually says the leak happens on the
     * snprintf line below. I'm not entirely sure what to make of it or do
     * about it currently */
    //create hashtable
    qhashtbl_t *hash = qhashtbl(7500,0);

    for(i = 0; i<n_files; i++){
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
        char word [ WORD_LEN ];
        int word_index = 0;
        while (fgets(line, MAX_LINE_LEN, file_reader) != NULL){
            for(j = 0; j < MAX_LINE_LEN; j++){
                if (line[j] == '<'){
                    while (j < MAX_LINE_LEN && line[j] != '>'){
                        j++;
                    }
                }else{
                    switch(line[j]){
                    //these cases are possible word delineators (not exhaustive)
                    case '\r':
                    case '\n':
                        j = MAX_LINE_LEN; //the end of the line has been reached
                    case '-':
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
                        //TODO this null check should probably be "word[0] != '\0'"
                        if (word != NULL && word_index != 0){
                            word[word_index] = '\0';
                            int n = hash->getint(hash, word);
                            hash->putint(hash, word, n+1);
                            word_index = 0;
                        }
                        break;
                    default:
                        word[word_index] = line[j];
                        word_index++;
                    }
                }

            }
        }

        //clean up
        fclose(file_reader);
    }
    /* this next section can and probably should be a separate method */

    int n_tallies = hash->size(hash);

    Tally *tallies = calloc(n_tallies, sizeof(Tally));

    qhashtbl_obj_t obj;
    //must be cleared before call
    memset((void*) &obj, 0, sizeof(obj));
    for (i = 0; hash->getnext(hash, &obj, true); i++){
        //dump hash keys:values into Tally array
        strcpy( tallies[i].word, obj.name);
        tallies[i].num = atoi((char*)obj.data); //ew. improve if possible
        free(obj.name);
        free(obj.data);
    }

    hash->free(hash);

    //sort this process's Tally array
    qsort(tallies, n_tallies, sizeof(Tally), tallyCmp);

    char file[11] = "allWords.x\0";

    FILE *out_file = fopen(file, "w");

    if (out_file == NULL){
        //TODO add error handling!
        printf("Error opening out_file in beginMergeProcess(). Aborting.\n");
        return -1;
    }
    int running_total;
    for(i = 0; i < n_tallies; i++){
        running_total = tallies[i].num;
        for(j = 1; j + i < n_tallies; j++){
            if(strcasecmp(tallies[i+j].word, tallies[i].word) == 0){
                running_total += tallies[i+j].num;
            }else{
                j--;
                break;
            }
        }
        fprintf(out_file, "%s : %d\n", tallies[i].word, running_total);
        i+=j;
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
        printf("Usage: run.o [count]\n");
        return -1;
    }


    //setup mpi derived type "Tally" used to transmit individual results
    //TODO REMOVE!!!!
    //TODO currently segfaults if n_files%16 != 0
    int n_files = atoi(argv[1]);

    struct timespec time_s, time_e, result;
    clock_gettime(CLOCK_MONOTONIC, &time_s);
    readAndTally(n_files);
    clock_gettime(CLOCK_MONOTONIC, &time_e);
    result.tv_nsec = time_e.tv_nsec - time_s.tv_nsec;
    if ( (time_e.tv_nsec - time_s.tv_nsec) < 0){
        result.tv_nsec += 1000000000;
    }

    printf("%d.%ld\n", (int)(floor(difftime(time_e.tv_sec, time_s.tv_sec))), result.tv_nsec);

//    printf("-------------------------Total time (proc %d): %f\n", world_rank, end_time - start_time);

//    if(world_rank == 0) muntrace(); //TODO REMOVE
    return 0;
}

