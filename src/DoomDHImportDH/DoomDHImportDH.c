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
#include <sqlite3.h>

#include "../Command.h"

#define DEBUG            0
#define BUFFER_SIZE      32760

char      inbuf[BUFFER_SIZE];
int       inbuflength;
char      outbuf[BUFFER_SIZE];
//char      **outbuf;  

char ip[16];
int  port;
int  ser;

int  nodeids[1024];
int  nodegeosize[1024];
int  nodecount;

pthread_t ntid; 
pthread_mutex_t mutex;
int  command_status; // >0--executing, 0--idle

sqlite3 *db;

#define TILE_COUNT 1024 * 128

int tiles[TILE_COUNT];
int tilegeosize[TILE_COUNT];
int tilenode[TILE_COUNT];
int tilecount;

int send_info(int socket, char command, char action, char* fmt, ...)
{
    int     n;
    va_list args;
    
    outbuf[0] = command;
    outbuf[1] = action;
    if(fmt != NULL && fmt[0] != '\0')
    {
        va_start(args, fmt);
        n = vsprintf(outbuf + 4, fmt, args);
        va_end(args);
        *(short*)(outbuf + 2)  = strlen(outbuf + 4) + 4 + 1; // + 1 for '\0'
    }
    else
    {
        *(short*)(outbuf + 2)  = 4;
    }
    send(socket,outbuf, *(short*)(outbuf + 2), 0);
}

void *handle_response(void *arg)
{
    fd_set  rdfs;
    int ser;
    int t, ret;
    int max_fd;
    
	  memset(inbuf, 0, BUFFER_SIZE);
    inbuflength = 0;
    while(1)
    {
        FD_ZERO(&rdfs);
        ser = *(int*)arg;
        if(ser == -1)
        {
            sleep(1);
            continue;
        }

        FD_SET(ser, &rdfs);
        max_fd = ser;
        ret = select(max_fd + 1,&rdfs,NULL, NULL, NULL);
        if(ret < 0)
        {  
            //printf("select error\n");  
        }
        else if(ret == 0)
        {
            //printf("time out\n");
        }
        else
        {
            if(FD_ISSET(ser, &rdfs)) //from server
            {
                ret = recv(ser,inbuf + inbuflength,BUFFER_SIZE,0);
                if(ret == 0)
                {
                    printf("Connection closed!\n");
                    close(ser);
                    ser = -1;
                }
                else if(ret < 0)
                {
                    printf("Error receive data \n");
                }
                else
                {
                    inbuflength += ret;
                    while(inbuflength >= 4)
                    {
                        t = *(short*)(inbuf + 2);
                        if(t > inbuflength)
                        {
                            break;
                        }
                        if(t > 4)
                        {
                            printf("%d,%d: %s\n", inbuf[0], inbuf[1], inbuf + 4);
                        }
                        else
                        {
                            printf("%d,%d\n", inbuf[0], inbuf[1]);
                        }
                        if(inbuf[0] == COMMAND_GETNODEINFO && inbuf[1] == COMMAND_ACTION_RETOUT_CLIENT)
                        {
                            char *p = inbuf + 4; // (ip:port)ID:XXX
                            p = strstr(p, ")");
                            if(p != NULL && strncasecmp(p + 1, "ID:", strlen("ID:")) == 0)
                            {
                                p += 1 + strlen("ID:");
                                nodeids[nodecount] = atoi(p); 
                                nodecount++;
                            }
                        }
                        if(inbuf[1] == COMMAND_ACTION_RETSTOP)
                        {
                            pthread_mutex_lock(&mutex);
                            command_status --;
                            pthread_mutex_unlock(&mutex);
                        }
                        if(DEBUG)
                        {
                            printf("inbuflength = %d, t = %d, command_status = %d\n", inbuflength, t, command_status);
                        }
                        memmove(inbuf, inbuf + t, inbuflength - t);
                        inbuflength -= t;
                    }
                }
            }
        }
    }
    return (void*)0;
}

int connect_to_server(char *ip, int port)
{
    int i = 0, ret;
    struct sockaddr_in seraddr;  
    seraddr.sin_family =AF_INET;  
    seraddr.sin_port   = htons(port);
    seraddr.sin_addr.s_addr=inet_addr(ip);
    ser = socket(AF_INET,SOCK_STREAM,0);  
    
    while((ret = connect(ser,(struct sockaddr*)&seraddr,sizeof(seraddr))) < 0)
    {
        printf("error:Failed to connect to server, ret = %d, errno=%d\n", ret, errno);
        i ++;
        ser = -1;
        if(i > 3)
        {
            break;
        }
        else
        {
            printf("try 5 second later ...\n");
            sleep(5);
        }
    }
    return ser;
}

int get_node_info()
{
    nodecount = 0;
    
    pthread_mutex_lock(&mutex);
    command_status++;
    pthread_mutex_unlock(&mutex);

    outbuf[0] = COMMAND_GETNODEINFO;
    outbuf[1] = COMMAND_ACTION_EXESTART;
    *(short*)(outbuf + 2) = 4;
    send(ser, outbuf, 4,0);

    sprintf(outbuf + 4, "ID");
    outbuf[0] = COMMAND_GETNODEINFO;
    outbuf[1] = COMMAND_ACTION_EXEINPUT;
     *(short*)(outbuf + 2) = strlen(outbuf + 4) + 4 + 1; //+1 for '\0'
    send(ser, outbuf, *(short*)(outbuf + 2), 0);

    outbuf[0] = COMMAND_GETNODEINFO;
    outbuf[1] = COMMAND_ACTION_EXESTOP;
    *(short*)(outbuf + 2) = 4;
    send(ser, outbuf, 4,0);
    
    while(command_status > 0)
    {
        usleep(1000);
    }
    return 1;
}

int main(int argc, char *argv[])
{
    int i, j, rc;
    sqlite3_stmt *statement;
    unsigned char *geo;
    int tileid;

    if(argc != 5)
    {
        printf("Usage: %s IP Port DHFile DHName\n", argv[0]);
        return -1;
    }
    
    strncpy(ip, argv[1], 15);
    port = atoi(argv[2]);
    ser  = connect_to_server(ip, port);
    if(ser == -1)
    {
        return -1;
    }
    
    command_status = 0;

    int temp;
    if(pthread_mutex_init(&mutex,NULL) != 0 )  
    {  
        printf("Init metux error.");  
        return -1;  
    }
    
    if((temp=pthread_create(&ntid,NULL,handle_response,(void*)&ser))!= 0)  
    {  
        printf("can't create thread: %s\n",strerror(temp));  
        return -1;  
    }  
    
    pthread_mutex_lock(&mutex);
    command_status++;
    pthread_mutex_unlock(&mutex);
    send_info(ser, COMMAND_CREATEDATABASE, COMMAND_ACTION_EXESTART, NULL);
    send_info(ser, COMMAND_CREATEDATABASE, COMMAND_ACTION_EXEINPUT, "%s", argv[4]);
    send_info(ser, COMMAND_CREATEDATABASE, COMMAND_ACTION_EXESTOP,  NULL);
    while(command_status > 0)
    {
        sleep(1);
    }

    pthread_mutex_lock(&mutex);
    command_status++;
    pthread_mutex_unlock(&mutex);
    send_info(ser, COMMAND_SETDATABASE, COMMAND_ACTION_EXESTART, NULL);
    send_info(ser, COMMAND_SETDATABASE, COMMAND_ACTION_EXEINPUT, "%s", argv[4]);
    send_info(ser, COMMAND_SETDATABASE, COMMAND_ACTION_EXESTOP,  NULL);
    while(command_status > 0)
    {
        sleep(1);
    }
    
    get_node_info();
    
    memset(nodegeosize, 0, sizeof(nodegeosize));
    memset(tiles, 0, sizeof(tiles));
    memset(tilegeosize, 0, sizeof(tilegeosize));
    memset(tilenode, 0, sizeof(tilenode));
    tilecount = 0;
    
    rc = sqlite3_open(argv[3], &db);
    if(rc != SQLITE_OK)
    {
        printf("error:Failed to open database %s", sqlite3_errmsg(db));
        return -1;
    }
    rc = sqlite3_prepare (db, "SELECT GeomWGS84 from DH_CHA;", -1, &statement, NULL);
    if(rc != SQLITE_OK)
    {
        printf("error:prepare error rc = %d!", rc);
        return;
    }
    while(sqlite3_step(statement) == SQLITE_ROW) 
    {
        //geo    = sqlite3_column_blob(statement, 0);
        //tileid = gettile(geo, 11);
        //i = findtile(tileid);
        //if(i == -1)
        //{
        //    i = inserttile(tileid);
        //}
        //tilegeosize[i] += sqlite3_column_bytes(statement, 0);
    }
    sqlite3_finalize(statement);
    
    //sorttilebygeosize();
    
    for(i = 0; i < tilecount; i++)
    {
        //j = findmingeosizenode();
        nodegeosize[j] += tilegeosize[i];
        tilenode[i] = nodeids[j];
    }

    //allocate memory for each node.
    //outbuf = (char **)(malloc(sizeof(char*)*nodecount));
    //for(i = 0; i < nodecount; i++)
    //{
    //    outbuf[i] = (char*)(malloc(sizeof(char) * BUFFER_SIZE));
    //}

    rc = sqlite3_prepare (db, "SELECT GeomWGS84 from DH_CHA;", -1, &statement, NULL);
    if(rc != SQLITE_OK)
    {
        printf("error:prepare error rc = %d!", rc);
        return;
    }
    while(sqlite3_step(statement) == SQLITE_ROW) 
    {
        //geo    = sqlite3_column_blob(statement, 100);
        //tileid = gettile(geo, 11);
        //i = findtile(tileid);
        
        //send the countent to tilenode[i]
        
    }
    sqlite3_finalize(statement);

    
    //free memory
    //for(i = 0; i < nodecount; i++)
    //{
    //    free(outbuf[i]);
    //}
    //free(outbuf);
    
    return  0;
}