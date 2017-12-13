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
#define BUFFER_SIZE      32760

char      inbuf[BUFFER_SIZE];
int       inbuflength;
char      outbuf[BUFFER_SIZE];  
pthread_t ntid; 

int split_string(char* buffer, char sperator, char** field, int *pos)
{
    int m = 0;
    int i = *pos;
    
    //skip /r /n;
    while(buffer[i] != '\0')
    {
        if(buffer[i] == '\r' || buffer[i] == '\n')
        {
            i++;
        }
        else
        {
            break;
        }
    }
    if(buffer[i] == '\0')
    {
        return 0;
    }
    
    field[m] = buffer + i;
    m++;
    while(buffer[i] != '\0' && buffer[i] != '\r' && buffer[i] != '\n')
    {
        if(buffer[i] == sperator)
        {
            buffer[i] = '\0';
            field[m] = buffer + i + 1;
            m++;
        }
        i++;
    }
    
    if(buffer[i] == '\r' || buffer[i] == '\n')
    {
        buffer[i] = '\0';
    }
    *pos = i + 1;
    
    return m;
}

void test_split()
{
    FILE *f;
    int i, m, n, pos, ret;
    f = fopen("./test.csv", "r");
    ret = fread(outbuf, 1, BUFFER_SIZE, f);
    outbuf[ret] = '\0';

    char *field[100];
    pos = 0;
    n = 0;
    printf("outbuf =\n%s\n", outbuf);
    while( (m = split_string(outbuf, ',', field, &pos)) > 0)
    {
        n++;
        printf("n = %d, m = %d, pos = %d\n", n, m, pos);
        for(i = 0; i < m; i++)
        {
            printf("Field[%d]=%s\n", i, field[i]);
        }
    }
    
    
}

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
                    printf("%d,%d: %s", inbuf[0], inbuf[1], inbuf + 4);
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
    int ret;
    struct sockaddr_in seraddr;  
    int ser;
    FILE *f;
    int  i, j, len;
    size_t t;
    char* line = NULL;
    
    //test_split();
    //return;
    
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
   
    outbuf[0] = COMMAND_ADDCHILDNODE;
    outbuf[1] = COMMAND_ACTION_EXESTART;
    *(short*)(outbuf + 2) = 4;
    send(ser, outbuf, 4,0);

    sprintf(outbuf + 4, "127.0.0.1 9000 9001");
    outbuf[0] = COMMAND_ADDCHILDNODE;
    outbuf[1] = COMMAND_ACTION_EXEINPUT;
    *(short*)(outbuf + 2) = strlen(outbuf + 4) + 4 + 1; //+1 for '\0'
    send(ser, outbuf, *(short*)(outbuf + 2), 0);

    sprintf(outbuf + 4, "127.0.0.1 9002 9003");
    outbuf[0] = COMMAND_ADDCHILDNODE;
    outbuf[1] = COMMAND_ACTION_EXEINPUT;
    *(short*)(outbuf + 2) = strlen(outbuf + 4) + 4 + 1; //+1 for '\0'
    send(ser, outbuf, *(short*)(outbuf + 2), 0);
    
    sleep(5); //wait for add node finishing
    printf("TTTTTTTTTTTTTTTTTTTTTTT\n");
    
    outbuf[0] = COMMAND_ADDCHILDNODE;
    outbuf[1] = COMMAND_ACTION_EXESTOP;
    *(short*)(outbuf + 2) = 4;
    send(ser, outbuf, 4,0);

    outbuf[0] = COMMAND_CREATEDATABASE;
    outbuf[1] = COMMAND_ACTION_EXESTART;
    *(short*)(outbuf + 2) = 4;
    send(ser, outbuf, 4,0);

    sprintf(outbuf + 4, "poi");
    outbuf[0] = COMMAND_CREATEDATABASE;
    outbuf[1] = COMMAND_ACTION_EXEINPUT;
    *(short*)(outbuf + 2) = strlen(outbuf + 4) + 4 + 1; //+1 for '\0'
    send(ser, outbuf, *(short*)(outbuf + 2), 0);

    outbuf[0] = COMMAND_CREATEDATABASE;
    outbuf[1] = COMMAND_ACTION_EXESTOP;
    *(short*)(outbuf + 2) = 4;
    send(ser, outbuf, 4,0);


    outbuf[0] = COMMAND_SETDATABASE;
    outbuf[1] = COMMAND_ACTION_EXESTART;
    *(short*)(outbuf + 2) = 4;
    send(ser, outbuf, 4,0);

    sprintf(outbuf + 4, "poi");
    outbuf[0] = COMMAND_SETDATABASE;
    outbuf[1] = COMMAND_ACTION_EXEINPUT;
    *(short*)(outbuf + 2) = strlen(outbuf + 4) + 4 + 1; //+1 for '\0'
    send(ser, outbuf, *(short*)(outbuf + 2), 0);

    outbuf[0] = COMMAND_SETDATABASE;
    outbuf[1] = COMMAND_ACTION_EXESTOP;
    *(short*)(outbuf + 2) = 4;
    send(ser, outbuf, 4,0);
   

    outbuf[0] = COMMAND_EXECUTEDDL;
    outbuf[1] = COMMAND_ACTION_EXESTART;
    *(short*)(outbuf + 2) = 4;
    send(ser, outbuf, 4,0);

    sprintf(outbuf + 4, "CREATE TABLE DH_POI(Id INTEGER, SupplierId  INTEGER,  Name   VARCHAR(320),  TransName  VARCHAR(320));");
    outbuf[0] = COMMAND_EXECUTEDDL;
    outbuf[1] = COMMAND_ACTION_EXEINPUT;
    *(short*)(outbuf + 2) = strlen(outbuf + 4) + 4 + 1; //+1 for '\0'
    send(ser, outbuf, *(short*)(outbuf + 2), 0);

    outbuf[0] = COMMAND_EXECUTEDDL;
    outbuf[1] = COMMAND_ACTION_EXESTOP;
    *(short*)(outbuf + 2) = 4;
    send(ser, outbuf, 4,0);


    sprintf(outbuf + 4, "DH_POI");
    outbuf[0] = COMMAND_IMPORTCSV;
    outbuf[1] = COMMAND_ACTION_EXESTART;
    *(short*)(outbuf + 2) = strlen(outbuf + 4) + 4 + 1; //+1 for '\0'
    send(ser, outbuf, *(short*)(outbuf + 2), 0);

    f = fopen("./test.csv", "r");
    int flen, readlen;
    
    outbuf[0] = COMMAND_IMPORTCSV;
    outbuf[1] = COMMAND_ACTION_EXEINPUT_ONE;
    
    if(f != NULL)
    {
        fseek(f, 0, SEEK_END);
        flen = ftell(f);
        fseek(f, 0, SEEK_SET);
        readlen = 0;
        j = 0;
        while(!feof(f))
        {
            memset(outbuf + 2, 0, BUFFER_SIZE - 2);
            ret = fread(outbuf + 6, 1, BUFFER_SIZE - 6, f);
            i = 0;
            readlen += ret;
            if(readlen < flen)
            {
                while(outbuf[6 + ret] != '\n' && outbuf[6 + ret] != '\r')
                {
                    ret --;
                    i++;
                }
                outbuf[6 + ret] = '\0';
            }
            else
            {
                outbuf[6 + ret + 1] = '\0';
            }
            
            *(short*)(outbuf + 2) = ret + 4 + 2 + 1;
            *(short*)(outbuf + 4) = (j % 2== 0) ? 100 : 101;
            send(ser, outbuf, *(short*)(outbuf + 2),0); 
            
            //printf("j = %d, outbuf = \n%s\n", j, outbuf + 6);
            
            if(readlen == flen)
            {
                break;
            }
            fseek(f, -i,SEEK_CUR);
            readlen -= i;
            j++;
        }
        fclose(f);
	  }
	  	  
    outbuf[0] = COMMAND_IMPORTCSV;
    outbuf[1] = COMMAND_ACTION_EXESTOP;
    *(short*)(outbuf + 2) = 4;
    send(ser, outbuf, 4,0);
	  
	  
    outbuf[0] = COMMAND_EXECUTEDQL;
    outbuf[1] = COMMAND_ACTION_EXESTART;
    *(short*)(outbuf + 2) = 4;
    send(ser, outbuf, 4,0);

    sprintf(outbuf + 4, "select count() from DH_POI;");
    outbuf[0] = COMMAND_EXECUTEDQL;
    outbuf[1] = COMMAND_ACTION_EXEINPUT;
    *(short*)(outbuf + 2) = strlen(outbuf + 4) + 4 + 1; //+1 for '\0'
    send(ser, outbuf, *(short*)(outbuf + 2), 0);

    outbuf[0] = COMMAND_EXECUTEDQL;
    outbuf[1] = COMMAND_ACTION_EXESTOP;
    *(short*)(outbuf + 2) = 4;
    send(ser, outbuf, 4,0);

    outbuf[0] = COMMAND_EXECUTEDML;
    outbuf[1] = COMMAND_ACTION_EXESTART;
    *(short*)(outbuf + 2) = 4;
    send(ser, outbuf, 4,0);

    sprintf(outbuf + 4, "create index dh_poi_id on DH_POI(id);");
    outbuf[0] = COMMAND_EXECUTEDML;
    outbuf[1] = COMMAND_ACTION_EXEINPUT;
    *(short*)(outbuf + 2) = strlen(outbuf + 4) + 4 + 1; //+1 for '\0'
    send(ser, outbuf, *(short*)(outbuf + 2), 0);

    outbuf[0] = COMMAND_EXECUTEDML;
    outbuf[1] = COMMAND_ACTION_EXESTOP;
    *(short*)(outbuf + 2) = 4;
    send(ser, outbuf, 4,0);

    
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