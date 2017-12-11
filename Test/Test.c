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


#define DEBUG            0
#define BUFFER_SIZE      1024

char      inbuf[BUFFER_SIZE];
char      outbuf[BUFFER_SIZE];  

pthread_t ntid; 


void *handle_response(void *arg)
{
    fd_set  rdfs;
    int ser;
    int ret;
    int max_fd;
    
    ser = *(int*)arg;
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
            	  memset(inbuf, 0, BUFFER_SIZE);
                ret = recv(ser,inbuf,BUFFER_SIZE,0);
                printf("\n");
                printf("%s", inbuf);
                printf("\n");
            }
        }
    }
    return (void*)0;
}

int main(int argc, char* argv[])  
{
    int ret;
    struct sockaddr_in seraddr;  
    int ser;
    FILE *f;
    
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
    
    int temp;
    if((temp=pthread_create(&ntid,NULL,handle_response,(void*)&ser))!= 0)  
    {  
        printf("can't create thread: %s\n",strerror(temp));  
        return 1;  
    }  
   
    strcpy(outbuf, "MNG:add child node\n\
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
                    /TSK\n\
                    TSK:import csv into DH_POI \n\
                    ");
    
    printf("%s",outbuf);
    send(ser, outbuf, strlen(outbuf),0);


    f = fopen("./test.csv", "r");
    int  i;
    char line[1024];
    
    i = 0;
    if(f != NULL)
    {
        while(!feof(f))
        {
            memset(line, 0, 1024);
            if(fgets(line, 1024, f) != NULL)
            {
                if(i == 0)
                {
                    i++;
                    continue; //omit first line;
                }
                if(i % 2 == 0)
                {
                    sprintf(outbuf, "#100:%s\n", line);
                }
                else
                {
                    sprintf(outbuf, "#101:%s\n", line);
                }
                send(ser, outbuf, strlen(outbuf),0); 
                i++;
            }
        }
        fclose(f);
	  }
    strcpy(outbuf, "/TSK\n\
                    TSK:execute dql\n\
                    select count() from DH_POI\n\
                    /TSK\n\
                    TSK:execute dml\n\
                    create index dh_poi_id on DH_POI(id);\n\
                    /TSK\n\
                    ");
    send(ser, outbuf, strlen(outbuf),0);
    
    while(gets(line))
    {
        sprintf(outbuf, "%s\n", line);
        send(ser, outbuf, strlen(outbuf),0);
        if(strcasecmp(line, "quit") == 0)
        {
            break;
        }
    }
    close(ser);    
}