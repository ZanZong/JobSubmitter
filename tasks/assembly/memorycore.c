#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
main(int argc,char *argv[]){
   char *p = (char*) malloc(atoi(argv[1])*1048576);
   usleep(atoi(argv[2])*1000);
   //while(true){}
   free(p);
   printf("malloc:%smb wait:%sms\n",argv[1],argv[2]); 
}
