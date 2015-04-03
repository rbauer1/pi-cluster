#include <stdio.h>
#include <mpi.h>
#include <string.h>
#include <stdlib.h>

int recvFiles(int world_rank){
    int buffer_size = 0, old_buffer_size = 0, i;
    char* incoming_file;
    MPI_Status status;

    for(i = 0; i < 12; i++){

        //printf("A");

        //find out how big file_buffer is using this and next call
        MPI_Probe(0,0, MPI_COMM_WORLD, &status);

        //printf("B");

        //get the file_buffer size and store in buffer_size
        MPI_Get_count(&status, MPI_CHAR, &buffer_size);

        if(buffer_size < 0){
            printf("Error: buffer_size = %d. Aborting.\n", buffer_size);
            return -1;
        }

//        printf("buffer size: %d, old buffer size: %d\n", buffer_size, old_buffer_size);
        if(i == 0){
            incoming_file = (char*)malloc(sizeof(char) * buffer_size);
//            printf("alloc\n");
        }else if(buffer_size > old_buffer_size){
//            printf("rea\n");
            incoming_file = (char*)realloc(incoming_file, sizeof(char) *
            buffer_size*2);
        }
//        printf("after %d\t", i);

        if(incoming_file == NULL){
            printf("Error: recvFiles malloc or realloc failed. Aborting\n");
            return -1;
        }

//        printf("C");

        MPI_Recv(incoming_file, buffer_size, MPI_CHAR, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

//        printf("D");

        //MPI_Get_count(&status, MPI_CHAR, &buffer_size);

        //printf("E");

        printf("Process %d received size %d file, old buffer: %d\n", world_rank, buffer_size, old_buffer_size);
        old_buffer_size = buffer_size;

    }
//    printf("before\n");
    free(incoming_file);
//    printf("after\n");
    return 0;
}

//--------------------------------------------------------------

int sendFiles(){
    const int MAX_LINE_LEN = 1001;
    const int MAX_LINES = 1025;
    double time_s, time_e;
    int i;

    char *file_buffer = (char*)malloc(sizeof(char) * MAX_LINE_LEN * MAX_LINES);

    FILE *out_file = fopen("results.txt", "a");

    if (out_file == NULL){
        printf("Error opening out_file. Aborting.\n");
        return -1;
    }

    for(i = 0; i < 12; i++){
        int line_start_index = 0;

        //length of file_name is max 16, eg files/file10.txt
        char file_name[17];

        //make file_name into "files/file"+i+".txt"
        snprintf(file_name, sizeof(file_name), "%s%d%s", "files/file", i, ".txt\0");

        FILE *file_reader = fopen(file_name, "rt");

        if(file_reader == NULL){
            printf("Error reading %s. Aborting.\n", file_name);
            return -1;
        }

        char line[MAX_LINE_LEN];
        while(fgets(line, MAX_LINE_LEN, file_reader) != NULL){
            strcpy(file_buffer + line_start_index, line);

            //strlen ought to always be 1000 here
            line_start_index += strlen(line) + 1;
        }

        printf("Sending file size %d\n", line_start_index);

        time_s = MPI_Wtime();
        MPI_Send(file_buffer, line_start_index, MPI_CHAR, 1, 0, MPI_COMM_WORLD);
        time_e = MPI_Wtime();
        //printf("Time to send (file %d), size %dB, to (proc 1 (ON machine)) is %f\n", i, line_start_index, time_e-time_s);
        fprintf(out_file, "%f\t", time_e-time_s);

        time_s = MPI_Wtime();
        MPI_Send(file_buffer, line_start_index, MPI_CHAR, 2, 0, MPI_COMM_WORLD);
        time_e = MPI_Wtime();
        //printf("Time to send (file %d), size %dB, to (proc 2 (OFF machine)) is %f\n", i, line_start_index, time_e-time_s);
        fprintf(out_file, "%f\n", time_e-time_s);

        fclose(file_reader);
    }
    fprintf(out_file, "!!!\n");
    fclose(out_file);
    free(file_buffer);
    return 0;
}

//--------------------------------------------------------------

int main(void){
    printf("Note: this should be run with 3 processes such that process 0 and 1 are on one machine, and 2 is on another machine\n");

    int world_rank, world_size;

    MPI_Init(NULL, NULL);

    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    int i;
    for(i=0; i < 1; i++){

        if (world_rank == 0){
            if(sendFiles() < 0){
                return -1;
            }
//            printf("send out\n");
        }else{
            if(recvFiles(world_rank) < 0){
                return -1;
            }
//            printf("recv out\n");
        }

    }

    MPI_Finalize();
    return 0;
}
