#include<stdlib.h>
#include<stdio.h>
#include<poll.h>  
#include<netdb.h>
#include<sys/types.h>  
#include<sys/socket.h>  
#include<arpa/inet.h>  
#include<unistd.h>  
#include<string.h>  
#include<errno.h>
#include<assert.h>
#include<pthread.h> 

#include "../Command.h"

#define DEBUG            0
//#define BUFFER_SIZE      32760
#define BUFFER_SIZE      2048

char      inbuf[BUFFER_SIZE];
int       inbuflength;
char      outbuf[BUFFER_SIZE];  
pthread_t ntid; 

void *handle_response(void *arg)
{
    fd_set  rdfs;
    int ser;
    int t, ret;
    int max_fd;
    
    ser = *(int*)arg;
	  memset(inbuf, 0, BUFFER_SIZE);
    inbuflength = 0;
    while(1)
    {
        FD_ZERO(&rdfs);
        //FD_SET(0, &rdfs);
        //FD_SET(1, &rdfs);
        FD_SET(ser, &rdfs);
        max_fd = ser;
        ret = select(max_fd + 1,&rdfs,NULL, NULL, NULL);
        if(ret < 0)
        {  
            printf("select error\n");  
        }
        else if(ret == 0)
        {
            printf("time out\n");
        }
        else
        {
            if(FD_ISSET(ser, &rdfs)) //from server
            {
                ret = recv(ser,inbuf + inbuflength,BUFFER_SIZE,0);
                inbuflength += ret;
                
                while(inbuflength > 4)
                {
                    t = *(short*)(inbuf + 2);
                    if(t > inbuflength)
                    {
                        break;
                    }
                    printf("%d,%d: %s\n", inbuf[0], inbuf[1], inbuf + 4);
                    memmove(inbuf, inbuf + t, inbuflength - t);
                    inbuflength -= t;
                }
            }
        }
    }
    return (void*)0;
}

int main(int argc, char* argv[])  
{
    int ser;
    struct sockaddr_in seraddr;  
    

    size_t len;
    ssize_t ret;
    char* line = NULL;
    
    seraddr.sin_family =AF_INET;  
    seraddr.sin_port   = htons(8000);
    seraddr.sin_addr.s_addr=inet_addr("127.0.0.1");
    ser = socket(AF_INET,SOCK_STREAM,0);  
    if(connect(ser,(struct sockaddr*)&seraddr,sizeof(seraddr)) < 0)
    {
        printf("error:Failed to connect to server\n");
        ser = -1;
    }
    
    int temp;
    if((temp=pthread_create(&ntid,NULL,handle_response,(void*)&ser))!= 0)  
    {  
        printf("can't create thread: %s\n",strerror(temp));  
        return 1;  
    }  

    printf("DoomDH>");
    while ((ret = getline(&line, &len, stdin)) != -1) 
    {
        //remove \r \n at the end
        line[ret - 1] = '\0';
        if(strcasecmp(line, "quit") == 0)
        {
            break;
        }
        printf("DoomDH>");
    }  
    close(ser);    
}