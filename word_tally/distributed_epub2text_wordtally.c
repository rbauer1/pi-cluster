#include <stdio.h>
#include <qlibc/qlibc.h>  //used for qhashtbl_t
#include <qlibc/containers/qhashtbl.h>
#include <mpi.h>
#include <string.h> //used for strcpy
#include <unistd.h>
#include <dirent.h> //used for counting files
#include <stdlib.h>

static const int LINE_LEN = 80;

int unzip(char *path){

    pid_t childpid;

    childpid = fork();

    if (childpid == -1){
        printf("Error in fork. Aborting.\n");
        return -1;
    }

    if (childpid == 0){
        //Child will handle unzipping the file
        execl("/usr/bin/unzip", "unzip", path, "OPS/main*", (char*) NULL);
    }else{
        //parent waits for child to finish before continuing
        wait(NULL);
        printf("Finished unzipping\n");
        return 0;
    }

}

int readAndSendFile(int n_files, int world_size){
    const int MAX_LINE_LEN = 256;
    const int MAX_LINES = 4096;

    int i;
    for(i = 0; i<n_files; i++){

        char *file_buffer = (char*)malloc(sizeof(char)*MAX_LINE_LEN*MAX_LINES);
        int n_lines = 0;
        /*  NOTE: this doesn't actually need to be an array.
            it is helpful because it allows each line to be accessed if need be
            but it would function just as well for normal running as a single
            int. Once everything else works, this should be changed to a plain
            int to avoid wasting a moderate amount of memory needlessly.
        */
        int line_start_index[MAX_LINES];

        //length of filename (should allow for 9999 pages)
        char p[17];

        //makes p into equivalent of "OPS/main"+i+".xml"
        snprintf(p, sizeof(p), "%s%d%s", "OPS/main",i,".xml\0");

        FILE *file_reader = fopen(p, "rt");

        if (file_reader == NULL){
            printf("Error opening in_file. Aborting.");
            //fclose(out_file);
            return -1;
        }

        char line[MAX_LINE_LEN], text[MAX_LINE_LEN];
        int stillOpen = 0;
        while (fgets(line, MAX_LINE_LEN, file_reader) != NULL){
            strcpy(file_buffer + line_start_index[n_lines], line);
            int len = strlen(line);
            n_lines++;
            line_start_index[n_lines] = line_start_index[n_lines-1] + len + 1;
            //printf("%d %s\n",line_start_index[n_lines], line);
            //stillOpen = parseLine(line, text, stillOpen);
        }

        printf("send buffer size = %d\n", line_start_index[n_lines]);

        MPI_Send(file_buffer, line_start_index[n_lines], MPI_CHAR, (i%(world_size-1))+1, 0, MPI_COMM_WORLD);

        //clean up
        fclose(file_reader);
        free(file_buffer);
    }
    return 0;
}

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

int convertFile2Plaintext(char *buffer, int buffer_size){
    //create hashtable
    qhashtbl_t *hash = qhashtbl(1000,0);

    //this will hold each word
    char word[50];

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
                //these cases are possible word delineators
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
    //wt is create file to write to, or erase everything in file
    //if it already exists
    FILE *out_file = fopen("words.txt", "w");

    if (out_file == NULL){
        printf("Error opening out_file. Aborting.");
        return -1;
    }
    qhashtbl_obj_t obj;
    //must be cleared before call
    memset((void*) &obj, 0, sizeof(obj));
    //see evernote (Todo) on ideas for this part and reducing
    while (hash->getnext(hash, &obj, true)){
        //dump hash keys:values into out_file
        fprintf(out_file, "%s:%s\n", obj.name, (char*)obj.data);
        free(obj.name);
        free(obj.data);
    }

    fclose(out_file);

    //not doing anything currently
    return 0;
}

int main(int argc, char* argv[]){

    if (argc < 2){
        printf("Usage: run [path/to/epub]\n");
        return -1;
    }

    MPI_Init(NULL, NULL);

    int world_rank, world_size;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    printf("Hello from pi %d, process: %ld\n", world_rank, (long)getpid());

    if (world_rank == 0){
        if (unzip(argv[1]) == -1){
            printf("Unzipping failed. Aborting.\n");
            return -1;
        }

        int n_files = countFiles("./OPS/");
        printf("Number of files: %d\n", n_files);

        readAndSendFile(3, world_size);
        MPI_Barrier(MPI_COMM_WORLD);

    }else{

        int buffer_size;
        MPI_Status status;
        char *incoming_file;

        //find out how big file_buffer is using this and next call
        MPI_Probe(0,0, MPI_COMM_WORLD, &status);

        //get file_buffer size and store in buffer_size
        MPI_Get_count(&status, MPI_CHAR, &buffer_size);

        printf("recv buffer_size = %d\n", buffer_size);

        if (buffer_size < 0){
            printf("Error: buffer_size = %d\n", buffer_size);
            return -1;
        }

        incoming_file = (char*)malloc(sizeof(char) * buffer_size);

        MPI_Recv(incoming_file, buffer_size, MPI_CHAR, 0, 0, MPI_COMM_WORLD, &status);

        //incoming_file[25] = '\0';

        //MPI_Get_count(&status, MPI_CHAR, &buffer_size);

        printf("pi: %d msg_len: %d msg: %s\n", world_rank, buffer_size, incoming_file);

        convertFile2Plaintext(incoming_file, buffer_size);

        free(incoming_file);

        //printf("pi: %d msg_len: %d msg: %s\n", world_rank, buffer_size, incoming_file);

        MPI_Barrier(MPI_COMM_WORLD);
    }


    MPI_Finalize();

    return(0);
}

