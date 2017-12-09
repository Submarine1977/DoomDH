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

#include "./Sqlite/sqlite3.h"

#define DEBUG            1
#define BUFFER_SIZE      1024
#define MAX_NODE_COUNT   1024
#define MAX_SERVER_COUNT 128
#define MAX_DB_COUNT     128
                              
int       serverid;                                   
char      inbuf[BUFFER_SIZE];
char      outbuf[BUFFER_SIZE];  

struct doom_dh_node
{
    int   socket;
    char  ip[16];
    int   port;   
    
    char  command[32];
    int   status;     //0--idle; 1--waiting for input; 2--waiting for result; 3--waiting for input & result
    char  buffer[BUFFER_SIZE*2];
};
struct    doom_dh_node*  psiblingnodes[MAX_NODE_COUNT];

struct doom_dh_server
{
    int   socket;
    char  ip[16];
    int   port;
    
    char  command[32];
    int   status;   //0--idle; 1--waiting for input; 2--waiting for result; 3--waiting for input & result
    char  buffer[BUFFER_SIZE*2];
};
struct    doom_dh_server*  pservers[MAX_SERVER_COUNT];


struct doom_dh_database
{
    sqlite3* db;
    char     name[128];
};
struct doom_dh_database  databases[MAX_DB_COUNT];
struct doom_dh_database  *pcurrentdb = NULL;


char * skip_space(char* str)
{
    while(*str == ' '	|| *str == '\t' || *str == '\r' || *str == '\n')
    {
        str++;
    }
    return str;
};

char * back_skip_space(char* end)
{
    while(*end == ' '	|| *end == '\t' || *end == '\r' || *end == '\n')
    {
        end--;
    }
    return end + 1;
};

void execute_ddl_dml(char* buffer, int server)
{
    int rc;
    char *errmsg;
    if(pcurrentdb == NULL)
    {
        sprintf(outbuf, "error:please set current database first!\n");
        send(server,outbuf, strlen(outbuf), 0);
        return;
    }
    rc = sqlite3_exec(pcurrentdb->db, buffer,NULL,NULL,&errmsg);
    if(rc == SQLITE_OK)
    {
        sprintf(outbuf, "OK!\n");
        send(server,outbuf, strlen(outbuf), 0);
    }
    else
    {
        sprintf(outbuf, "error:%s\n", errmsg);
        send(server,outbuf, strlen(outbuf), 0);
    }
};

void execute_dql(char* buffer, int server)
{
    int i, n, rc;
    sqlite3_stmt *statement;

    if(pcurrentdb == NULL)
    {
        sprintf(outbuf, "error:please set current database first!\n");
        send(server,outbuf, strlen(outbuf), 0);
        return;
    }
    rc = sqlite3_prepare (pcurrentdb->db, buffer, -1, &statement, NULL);
    if(rc != SQLITE_OK)
    {
        sprintf(outbuf, "error:prepare error rc = %d!", rc);
        send(server,outbuf, strlen(outbuf), 0);
    }
    
    send(server,outbuf, strlen(outbuf), 0);
    n = sqlite3_column_count(statement);
    while(sqlite3_step(statement) == SQLITE_ROW) 
    {
    	  memset(outbuf, 0, BUFFER_SIZE);
        for(i = 0; i < n; i++)
        {
            strcat(outbuf, (char *)sqlite3_column_text(statement, 1));
            if(i < n)
            {
                strcat(outbuf, "\t");
            }
            else
            {
                strcat(outbuf, "\n");
            }
            send(server,outbuf, strlen(outbuf), 0);
        }
    }
    sqlite3_finalize(statement);
    send(server,outbuf, strlen(outbuf), 0);
};

struct doom_dh_database* set_database(char* buffer, int server)
{
    int i;
    char *name;
    name = buffer;
    while(*name == ' ')
        name++;
    i = 0;
    while(name[i] != ' ' && name[i] != '\t' && name[i] != '\r' && name[i] != '\n' && name[i] != '\0')
    {
        i++;
    }
    name[i] = '\0';

    for(i = 0; i < MAX_DB_COUNT; i++)
    {
        if(databases[i].db != NULL && strcasecmp(databases[i].name, name) == 0)
        {
            sprintf(outbuf, "Database %s was set as current database!\n", name);
            send(server,outbuf, strlen(outbuf), 0);
            pcurrentdb = &databases[i];
            return &databases[i];
        }
    }
    sprintf(outbuf, "error:could not find the databases %s\n", name);
    send(server,outbuf, strlen(outbuf), 0);
    return NULL;
}

struct doom_dh_database* create_database(char* buffer, int server)
{
    int i, rc;
    char *name;
    name = buffer;
    while(*name == ' ')
        name++;
    i = 0;
    while(name[i] != ' ' && name[i] != '\t' && name[i] != '\r' && name[i] != '\n' && name[i] != '\0')
    {
        i++;
    }
    name[i] = '\0';
    
    for(i = 0; i < MAX_DB_COUNT; i++)
    {
        if(databases[i].db == NULL)
        {
            break;
        }
    }
    
    if(i < MAX_DB_COUNT)
    {
        strcpy(databases[i].name, name);
        rc = sqlite3_open(":memory:", &databases[i].db);
        if(rc == SQLITE_OK)
        {
            sprintf(outbuf, "Database %s was created!\n", name);
            send(server,outbuf, strlen(outbuf), 0);
            return &databases[i];
        }
        else
        {
            sprintf(outbuf, "error:Failed to open database %s\n", sqlite3_errmsg(databases[i].db));
            send(server,outbuf, strlen(outbuf), 0);
            databases[i].db = NULL;
            databases[i].name[0] = '\0';
            return NULL;
        }
    }
    else
    {
        sprintf(outbuf, "error:Failed to create database, too many databases\n");
        send(server,outbuf, strlen(outbuf), 0);
        return NULL;
    }
};

void get_node_info(char* buffer, int server)
{
    int i;
    char *name;
    name = buffer;
    while(*name == ' ')
        name++;
    i = 0;
    while(name[i] != ' ' && name[i] != '\t' && name[i] != '\r' && name[i] != '\n' && name[i] != '\0')
    {
        i++;
    }
    name[i] = '\0';

    if(strcasecmp(name, "ID") == 0)
    {
        sprintf(outbuf, "%d\n", serverid);
        send(server,outbuf, strlen(outbuf), 0);
        return;
    }
    sprintf(outbuf, "error:do not have the %s infomation\n", name);
    send(server,outbuf, strlen(outbuf), 0);
}
struct doom_dh_node* add_sibling_node(char* buffer, int server)
{
    int j, ret;
    char* ip;
    char* port;
    struct sockaddr_in nodeaddr;  
                    
    ip  = buffer;
    while(*ip == ' ') 
        ip++;
    port = ip;
    while(*port != ':')
    {
        if(*port == '\0')
        {
            sprintf(outbuf, "error:Wrong sibling node format%s\n", buffer);
            send(server,outbuf, strlen(outbuf), 0);
            return NULL;
        }	
        port++;
    }
    *port = '\0';
    port ++;
    
    for(j = 0; j < MAX_NODE_COUNT; j++)
    {
        if(psiblingnodes[j] != NULL)
        {
            if( strcmp(psiblingnodes[j]->ip, ip) == 0 && 
            	  atoi(port) == psiblingnodes[j]->port )
            {
                break;
            }
        }
    }
    if(j < MAX_NODE_COUNT)
    {
        sprintf(outbuf, "error:Failed to add sibling node %s %s, node was already connected, or port was already used\n", ip, port);
        send(server,outbuf, strlen(outbuf), 0);
        return NULL;
    }
    
                    
    for(j = 0; j < MAX_NODE_COUNT; j++)
    {
        if(psiblingnodes[j] == NULL)
        {
            psiblingnodes[j] = (struct doom_dh_node*)malloc(sizeof(struct doom_dh_node));
            break;
        }
    }
                    
    if(j < MAX_NODE_COUNT)
    {
        strcpy(psiblingnodes[j]->ip, ip);
        psiblingnodes[j]->port = atoi(port);
        psiblingnodes[j]->socket = socket(AF_INET,SOCK_STREAM,0);

        nodeaddr.sin_family =AF_INET;  
        nodeaddr.sin_port   = htons(psiblingnodes[j]->port);
        nodeaddr.sin_addr.s_addr=inet_addr(ip);
        ret = connect(psiblingnodes[j]->socket,(struct sockaddr*)&nodeaddr,sizeof(nodeaddr));
        if(ret < 0)
        {
            free(psiblingnodes[j]);
            psiblingnodes[j] = NULL;
            sprintf(outbuf, "error: Failed to add sibling node, %s:%s, errno=%d\n", ip, port, errno);
            send(server,outbuf, strlen(outbuf), 0);
            return NULL;
        }
        else
        {
            sprintf(outbuf, "New sibling node added, %s:%s\n", ip, port);
            send(server,outbuf, strlen(outbuf), 0);
            return psiblingnodes[j];
        }
        
    }
    else
    {
        sprintf(outbuf, "error:Failed to add sibling node, too many nodes\n");
        send(server,outbuf, strlen(outbuf), 0);
        return NULL;
    }
};

//handle server request
void handle_server_request(struct doom_dh_server* pser)
{
		int  j, t;
    char *str, *start, *end;
    
    while(1)
    {
        //get one line
        str   = skip_space(pser->buffer);
        start = end = str;
        while(*end != '\r' && *end != '\n' && *end != '\0')
        {
           end ++;
        }
        if(*end == '\0')
        {//line not finished
           return;
        }

        //get one line from "start" to "end"
        str = skip_space(end + 1);
        
        //trim space backward
        end = back_skip_space(end);
        *end = '\0';

        if(pser->status == 0) //idle
        {
            if(strncasecmp(start, "MNG:", strlen("MNG:")) == 0)
            {
                strcpy(pser->command, skip_space(start + strlen("MNG:")));
                if( strcasecmp(pser->command, "add sibling node") != 0 && 
                	  strcasecmp(pser->command, "get node id") != 0)
                {
                    printf("error MNG command %s\n", pser->command);
                }
                else
                {
                    pser->status = 1;
                    sprintf(outbuf, "RES:%s\n", pser->command);
                    send(pser->socket,outbuf, strlen(outbuf), 0);
                }
            }
            else if(strncasecmp(start, "TSK:", strlen("TSK:")) == 0)
            {
                strcpy(pser->command, skip_space(start + strlen("TSK:")));
                
                if( strcasecmp(pser->command, "create database")  != 0 &&
                	  strcasecmp(pser->command, "set database")     != 0 &&
                	  strcasecmp(pser->command, "execute ddl")      != 0 &&
                	  strcasecmp(pser->command, "execute dml")      != 0 &&
                	  strcasecmp(pser->command, "execute dql")      != 0 &&
                	  strcasecmp(pser->command, "import csv")      != 0 )
                {
                    printf("error TSK command %s\n", pser->command);
                }
                else
                {
                    sprintf(outbuf, "RES:%s\n", pser->command);
                    send(pser->socket,outbuf, strlen(outbuf), 0);
                    pser->status = 1;
                }
            }
            else
            {
                printf("error request %s\n", start);
            }
        }
        else if(pser->status & 1) //waiting for input
        {
            if(strcasecmp(start, "/MNG") != 0 && strcasecmp(start, "/TSK") != 0 )
            {	
                if(strcasecmp(pser->command, "add sibling node") == 0)	
                {
                    add_sibling_node(start, pser->socket);
                }
                else if(strcasecmp(pser->command, "get node info") == 0)	
                {
                    get_node_info(start, pser->socket);
                }
                else if(strcasecmp(pser->command, "create database") == 0)	
                {
                    create_database(start, pser->socket);
                }
                if(strcasecmp(pser->command, "set database") == 0)	
                {
                    set_database(start, pser->socket);
                }
                else if(strcasecmp(pser->command, "execute ddl") == 0)	
                {
                    execute_ddl_dml(start, pser->socket);
                }
                else if(strcasecmp(pser->command, "execute dml") == 0)	
                {
                    execute_ddl_dml(start, pser->socket);
                }
                else if(strcasecmp(pser->command, "execute dql") == 0)	
                {
                    execute_dql(start, pser->socket);
                }
                else if(strcasecmp(pser->command, "import csv") == 0)	
                {
                    execute_dql(start, pser->socket);
                }
                else
                {
                    printf("error request %s, command=%s\n", start, pser->command);
                }
            }
            else
            {
                sprintf(outbuf, "/RES\n");
                send(pser->socket,outbuf, strlen(outbuf), 0);
                pser->status &= 2; //no need to wait for input
            }
        }
        else
        {
            printf("node is not ready to handle request status=%d, request=%s\n", pser->status, start);
        }
        strcpy(pser->buffer, str); //handle one line
    }
};

//handle node request
void handle_node_request(struct doom_dh_node* pnode)
{
};

int main(int argc, char* argv[])  
{
    int    i, j, ret;
    int    ser,node,cli;  
    struct sockaddr_in seraddr,nodeaddr, cliaddr;  
    fd_set  rdfs;
    int     serverport, nodeport;
    
    if(argc != 5)
    {
        printf("Usage: %s ID IP SERVER_PORT NODE_PORT\n", argv[0]);
        return 1;
    }
    
    serverid   = atoi(argv[1]);
    serverport = atoi(argv[3]);
    nodeport   = atoi(argv[4]);

    socklen_t clilen        = sizeof(cliaddr);  
 
    seraddr.sin_family      = AF_INET;  
    //seraddr.sin_addr.s_addr = htonl(INADDR_ANY);  
    seraddr.sin_addr.s_addr = inet_addr(argv[2]);
    seraddr.sin_port        = htons(serverport);  
    ser    = socket(AF_INET,SOCK_STREAM,0);  
    ret    = bind(ser,(struct sockaddr*)&seraddr,sizeof(seraddr));  

    ret    = listen(ser,5); 
    if(ret < 0)
    {
        printf("Failed to listen on port %d, errno = %d\n", serverport, errno);
        return 1;
    }

    nodeaddr.sin_family      = AF_INET;  
    nodeaddr.sin_addr.s_addr = htonl(INADDR_ANY);  
    nodeaddr.sin_port        = htons(nodeport);  
    node   = socket(AF_INET,SOCK_STREAM,0);  
    ret    = bind(node,(struct sockaddr*)&nodeaddr,sizeof(nodeaddr));  

    ret    = listen(node,5); 
    if(ret < 0)
    {
        printf("Failed to listen on port %d, errno = %d\n", nodeport, errno);
        return 1;
    }
    
    printf("Listening server on %s:%s...\n", argv[2], argv[3]);
    printf("Listening node on %s:%s...\n", argv[2], argv[4]);

    
    for(i = 0; i < MAX_DB_COUNT; i++)
    {
        databases[i].db = NULL;
        databases[i].name[0] = '\0';
    }
    for( i = 0; i < MAX_NODE_COUNT; i++)
    {
        psiblingnodes[i] = NULL;
    }
    for(i = 0; i < MAX_SERVER_COUNT; i++)
    {
        pservers[i] = NULL;
    }

    while(1)  
    {   
        int max_fd = -1;
        FD_ZERO(&rdfs);
        FD_SET(ser, &rdfs); 
        FD_SET(node, &rdfs); 
        max_fd = ser > node ? ser : node;

        //dump client and node socket status
        if(DEBUG)
        {
            printf("=============Loop started===============\n"	);
            printf("\nserver socket status:\n");
            for(i = 0; i < MAX_SERVER_COUNT; i++)
            {
                if(pservers[i] != NULL)
                {
                    printf("pservers[%d](%s:%d),status=%d\n", i, pservers[i]->ip, pservers[i]->port, pservers[i]->status);
                }
            }
            printf("\nsibling node socket status:\n");
            for(j = 0; j < MAX_NODE_COUNT; j++)
            {
                if(psiblingnodes[j] != NULL)
                {
                    printf("psiblingnodes[%d](%s:%d),status=%d\n", j, psiblingnodes[j]->ip, psiblingnodes[j]->port, psiblingnodes[j]->status);
                }
            }
        }
        //dump end
        
        //check if there is client server for input or result
        struct doom_dh_server *pser = NULL;
        for(i = 0; i < MAX_SERVER_COUNT; i++)
        {
            if(pservers[i] != NULL && pservers[i]->status != 0)
            {
                pser = pservers[i];
                break;
            }
        }
        if(pser == NULL) //there is no client waiting for input or result, receive msg from all client
        {	
            for(i = 0; i < MAX_SERVER_COUNT; i++)
            {
                if(pservers[i] != NULL)
                {
                    FD_SET(pservers[i]->socket, &rdfs);
                    max_fd = pservers[i]->socket > max_fd ? pservers[i]->socket : max_fd;
                }
            }
        }
        else // there is one server who is waiting for input or result.
        {
            if(pser->status & 1) //receive msg from this server when it is also waiting for input
            {
                FD_SET(pser->socket, &rdfs);
                max_fd = pser->socket > max_fd ? pser->socket : max_fd;
                        	
            }
        }
        for(i = 0; i < MAX_NODE_COUNT; i++)
        {
            if(psiblingnodes[i] != NULL && (psiblingnodes[i]->status & 2)) //only receive the msg from nodes who are waiting for result. 
            {
                FD_SET(psiblingnodes[i]->socket, &rdfs);
                max_fd = psiblingnodes[i]->socket > max_fd ? psiblingnodes[i]->socket : max_fd;
            }
        }
        ret = select(max_fd + 1,&rdfs,NULL, NULL, NULL);

        //dump select result
        if(DEBUG)
        {
            printf("\nselect result:\n");
            for(i = 0; i < MAX_SERVER_COUNT; i++)
            {
                if(pservers[i]!= NULL && FD_ISSET(pservers[i]->socket, &rdfs))
                {
                    printf("pservers[%d] is selected\n", i);;
                }
            }
            for(j = 0; j < MAX_NODE_COUNT; j++)
            {
                if(psiblingnodes[j] != NULL && FD_ISSET(psiblingnodes[j]->socket, &rdfs))
                {
                    printf("childnode[%d] is selected\n", j);
                }
            }
        }

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
            memset(inbuf,0,BUFFER_SIZE);
            memset(outbuf, 0, BUFFER_SIZE);
            if(FD_ISSET(ser, &rdfs))  
            {  
                cli=accept(ser,(struct sockaddr*)&cliaddr,&clilen);
                if(cli > 0)
                {
                    for(i = 0; i < MAX_SERVER_COUNT; i++)
                    {
                        if(pservers[i] == NULL)
                        {
                            pservers[i] = (struct doom_dh_server*)malloc(sizeof(struct doom_dh_server));
                            pservers[i]->socket = cli;
                            strcpy(pservers[i]->ip,inet_ntoa(cliaddr.sin_addr));
                            pservers[i]->port = ntohs(cliaddr.sin_port);
                            pservers[i]->command[0] = '\0';
                            pservers[i]->status     = 0;
                            pservers[i]->buffer[0]  = '\0';
                            break;
                        }
                    }
                    if(i == MAX_SERVER_COUNT)
                    {
                        sprintf(outbuf, "error: failed to connect to the server, too many connections\n");
                        send(cli,outbuf, strlen(outbuf), 0);
                        close(cli);
                    }
                    else
                    {
                        printf("Connected to %s:%d\n",inet_ntoa(cliaddr.sin_addr),  
                                ntohs(cliaddr.sin_port));
                    
                    }
                }
            }
            else if(FD_ISSET(node, &rdfs))
            {
                cli=accept(node,(struct sockaddr*)&cliaddr,&clilen);
                if(cli > 0)
                {
                    for(i = 0; i < MAX_NODE_COUNT; i++)
                    {
                        if(psiblingnodes[i] == NULL)
                        {
                            psiblingnodes[i] = (struct doom_dh_node*)malloc(sizeof(struct doom_dh_node));
                            psiblingnodes[i]->socket = cli;
                            strcpy(psiblingnodes[i]->ip, inet_ntoa(cliaddr.sin_addr));
                            psiblingnodes[i]->port = ntohs(cliaddr.sin_port);
                            psiblingnodes[i]->command[0] = '\0';
                            psiblingnodes[i]->status     = 0;
                            psiblingnodes[i]->buffer[0]  = '\0';
                            break;
                        }
                    }
                    if(i == MAX_NODE_COUNT)
                    {
                        sprintf(outbuf, "error:Failed to connect to sibling node, too many connections\n/ERR\n");
                        send(cli,outbuf, strlen(outbuf), 0);
                        close(cli);
                    }
                    else
                    {
                        printf("connect to sibling node %s:%d\n",inet_ntoa(cliaddr.sin_addr),  
                                ntohs(cliaddr.sin_port));
                    }
                }
            }
            else        
            {
                for(i = 0; i < MAX_SERVER_COUNT; i++)
                {
                    if(pservers[i]!= NULL && FD_ISSET(pservers[i]->socket, &rdfs))
                    {
                        cli = pservers[i]->socket;	
                        break;
                    }
                }
                if(i >= MAX_SERVER_COUNT)
                {
                    for(j = 0; j < MAX_NODE_COUNT; j++)
                    {
                        if(psiblingnodes[j] != NULL && FD_ISSET(psiblingnodes[j]->socket, &rdfs))
                        {
                            cli = psiblingnodes[j]->socket;
                            break;
                        }
                    }
                    assert(j < MAX_NODE_COUNT);
                }
                ret = recv(cli,inbuf,BUFFER_SIZE,0);
            	  if(ret == 0)
            	  {   
            	      close(cli);
            	      if(i < MAX_SERVER_COUNT)
            	      {
                        printf("Client connection %s:%d closed!\n", pservers[i]->ip, pservers[i]->port);
                        free(pservers[i]);
                        pservers[i] = NULL;
            	      }
            	      else
            	      {
            	          printf("Connection %s:%d closed!\n", psiblingnodes[j]->ip, psiblingnodes[j]->port);
            	          assert(j < MAX_NODE_COUNT);
            	          free(psiblingnodes[j]);
            	          psiblingnodes[j] = NULL;
            	      }
            	      continue;
            	  }
            	  else if(ret < 0)
            	  {
            	      printf("error recieve data, errno = %d\n", errno);
            	      continue;
                }
                if(DEBUG)
                {
                    printf("%s", inbuf);
                }
                //ret > 0
                if(i < MAX_SERVER_COUNT)
                {
                    if( !(pservers[i]->status & 1) && //not waiting for input
                	      strncasecmp(inbuf, "quit", strlen("quit")) == 0 )
                    {
                        close(cli);
            	          printf("Connection %s:%d closed!\n", pservers[i]->ip, pservers[i]->port);
                        free(pservers[i]);
                        pservers[i] = NULL;
                        continue;
                    }
                }
                else
                {
                    if(	!(psiblingnodes[i]->status & 2) && //not waiting for input
                	      strncasecmp(inbuf, "quit", strlen("quit")) == 0 )
                	  {
                        close(cli);
            	          printf("Connection %s:%d closed!\n", psiblingnodes[j]->ip, psiblingnodes[j]->port);
            	          assert(j < MAX_NODE_COUNT);
            	          free(psiblingnodes[j]);
            	          psiblingnodes[j] = NULL;
                        continue;
                    }
                }
                
                if(i < MAX_SERVER_COUNT)
                {//request from client
                    strcat(pservers[i]->buffer, inbuf);
                    handle_server_request(pservers[i]);
                }
                else
                {//response from node
                    strcat(psiblingnodes[j]->buffer, inbuf);
                    handle_node_request(psiblingnodes[j]);
                }
            }
        }
    }
    return 1;  
}