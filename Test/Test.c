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

#define DEBUG            1
#define BUFFER_SIZE      1024

char      inbuf[BUFFER_SIZE];
char      outbuf[BUFFER_SIZE];  

int main(int argc, char* argv[])  
{
    int max_fd;
    int ret;
    struct sockaddr_in seraddr;  
    fd_set  rdfs;
    int ser;
    
    seraddr.sin_family =AF_INET;  
    seraddr.sin_port   = htons(8000);
    seraddr.sin_addr.s_addr=inet_addr("127.0.0.1");
    ser = socket(AF_INET,SOCK_STREAM,0);  
    ret = connect(ser,(struct sockaddr*)&seraddr,sizeof(seraddr));
    if(ret < 0)
    {
        printf("error:Failed to connect to server\n");
        return 1;
    }
    
   
    strcat(outbuf, "MNG:add child node\n\
                    127.0.0.1 9000 9001\n\
                    127.0.0.1 9002 9003\n\
                    /MNG\n\
                    MNG:get node info\n\
                    id\n\
                    /MNG\n\
                    TSK:create database\n\
                    poi\n\
                    /TSK\n\
                    TSK:set database\n\
                    poi\n\
                    /TSK\n\
                    TSK:execute ddl\n\
                    CREATE TABLE DH_POI(Id INTEGER, SupplierId  INTEGER,  Name   VARCHAR(320),  TransName  VARCHAR(320));\n\
                    /TSK\n");
    
    printf("%s",outbuf);
    send(ser, outbuf, strlen(outbuf),0);

    while(1)
    {
        FD_ZERO(&rdfs);
        FD_SET(0, &rdfs);
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
            if(FD_ISSET(1, &rdfs))  //from std
            {
                fread(outbuf, BUFFER_SIZE, 1, stdin);
                send(ser, outbuf, strlen(outbuf),0);
            }
            else if(FD_ISSET(ser, &rdfs)) //from server
            {
                ret = recv(ser,inbuf,BUFFER_SIZE,0);
                printf("%s", inbuf);
            }
        }
    }
    close(ret);    
}