#include <stdio.h>
#include <qlibc/qlibc.h>
#include <qlibc/containers/qhashtbl.h>
#include <string.h>
#include <stdlib.h>

int test(){
    qhashtbl_t *hash = qhashtbl(100,0);

    char *str = "<tags>words</tags> i am a string\n with a newline and\t i have tabs, i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i i\0";

    int len = (int)strlen(str) + 1;
    int i, word_index;
    word_index = 0;
    char word[50];

    for(i = 0; i < len; i++){
        if (str[i] == '<'){
            while (i < len && str[i] != '>'){
                i++;
            }
        }else{
            switch(str[i]){
            case '\n':
            case ' ':
            case '\t':
            case '\0':
            case ',':
            case '.':
            case ':':
            case ';':
                if (word != NULL && word_index != 0){
                    word[word_index] = '\0';
                    int n = hash->getint(hash, word);
                    hash->putint(hash, word, ++n);
                    word_index = 0;
                }
                break;
            default:
                word[word_index] = str[i];
                word_index++;
            }
        }
    }
    int num_objs = 0;
    //hash->debug(hash, stdout);
    qhashtbl_obj_t obj;
    //must be cleared before call
    memset((void*) &obj, 0, sizeof(obj));
    while (hash->getnext(hash, &obj, true)){
        printf("%s:%s num_objs:%d\n", obj.name, (char*)obj.data, ++num_objs);
        free(obj.name);
        free(obj.data);
    }
    printf("hash->size(hash) = %d\n",(int)hash->size(hash));
    return 0;
}


void test2(){
    qhashtbl_t *hash = qhashtbl(100,0);
    int p = hash->getint(hash, "test");
    printf("%d\n",p);
    p++;
    hash->putint(hash, "test", p);
    int q = hash->getint(hash, "test");
    printf("%d\n", q);
    q++;
    hash->putint(hash, "test", q);
    p = hash->getint(hash, "test");
    printf("%d\n",p);

}

void test1(){
    int vals[100];
    int i;
    for(i = 0; i<100; i++){
        vals[i] = 0;
    }
    printf("1\n");
    qhashtbl_t *hash = qhashtbl(100,0);
    printf("2 val[0]=%d\n", vals[0]);
    hash->put(hash, "test", &vals[0], sizeof(int));
    printf("3\n");
    printf("%d\n",atoi((char*)hash->get(hash, "test",NULL,false)));
    printf("4\n");

}

int main(void){
    test();
    return 0;
}
