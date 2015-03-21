#include <stdio.h>
#include <stdlib.h>

static const int LINE = 20;

int main(){
    char str[LINE];
    printf("%d\n",(int)sizeof(str));
    char *newStr = (char*)malloc(sizeof(str));
    printf("%d\n",(int)sizeof(newStr));
    free(newStr);
    return 0;
}
