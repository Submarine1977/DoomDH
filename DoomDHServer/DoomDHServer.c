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
#define MAX_NODE_COUNT   1024
#define MAX_CLIENT_COUNT 128

char      inbuf[BUFFER_SIZE];
char      outbuf[BUFFER_SIZE];  

int commandid = 1;
struct doom_dh_command
{
    int  id;
    int  parentid;
    char name[32];
    int  waitinginput;
    int  waitingresult;
};

struct doom_dh_command_list
{
    struct doom_dh_command command;
    struct doom_dh_command_list *prev, *next;
};

struct doom_dh_node
{
    int   id;	
    int   socket;
    char  ip[16];
    int   serverport;   
    int   nodeport;
    
    struct doom_dh_command_list *firstcommand,*lastcommand;
    char  buffer[BUFFER_SIZE*2];
};
struct    doom_dh_node*  pchildnodes[MAX_NODE_COUNT];

struct doom_dh_client
{
    int   socket;
    char  ip[16];
    int   port;
    
    struct doom_dh_command_list *firstcommand,*lastcommand;
    char  buffer[BUFFER_SIZE*2];
};
struct    doom_dh_client*  pclients[MAX_CLIENT_COUNT];

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


void add_childcommand(struct doom_dh_node* pnode, struct doom_dh_command_list *pcommand)
{
    if(pnode->firstcommand == NULL)
    {
        pnode->firstcommand = pnode->lastcommand = pcommand;
    }
    else
    {
        pnode->lastcommand->next = pcommand;
        pcommand->prev = pnode->lastcommand;
        pnode->lastcommand = pcommand;
    }
}

void add_command(struct doom_dh_client* pclient, struct doom_dh_command_list *pcommand)
{
    if(pclient->firstcommand == NULL)
    {
        pclient->firstcommand = pclient->lastcommand = pcommand;
    }
    else
    {
        pclient->lastcommand->next = pcommand;
        pcommand->prev = pclient->lastcommand;
        pclient->lastcommand = pcommand;
    }
}

struct doom_dh_command_list *get_command(int commandid, struct doom_dh_client **ppcli)
{
    int i;
    struct doom_dh_command_list* pcommand = NULL;
    for(i = 0; i < MAX_CLIENT_COUNT; i++)
    {
        pcommand = pclients[i]->firstcommand;
        while(pcommand != NULL)
        {
            if(pcommand->command.id == commandid)
            {
                *ppcli = pclients[i];
                return pcommand;
            }
        }
    }
    *ppcli = NULL;
    return NULL;
}


void dump_command_status(struct doom_dh_command_list *pcommand)
{
    while(pcommand != NULL)
    {
        printf("%d,%d,%s,%d,%d\n", pcommand->command.id, pcommand->command.parentid, pcommand->command.name, pcommand->command.waitinginput, pcommand->command.waitingresult);
        pcommand = pcommand->next;
    }
}

struct doom_dh_node* add_child_node(char* buffer, int server)
{
    int j, ret;
    char *ip;
    char *serverport, *nodeport;
    struct sockaddr_in nodeaddr;  

    ip = skip_space(buffer);

    serverport = ip;
    while(*serverport != ' ') 
    {
        if(*serverport == '\0')
        {
            sprintf(outbuf, "error:wrong child node format: %s\n", buffer);
            send(server,outbuf, strlen(outbuf), 0);
            return NULL;
        }
        serverport++;
    }
    *serverport = '\0';
    serverport++;
    
    nodeport = serverport + 1;
    while(*nodeport != ' ') 
    {
        if(*nodeport == '\0')
        {
            sprintf(outbuf, "error:wrong child node format: %s\n", buffer);
            send(server,outbuf, strlen(outbuf), 0);
            return NULL;
        }
        nodeport++;
    }
    *nodeport = '\0';
    nodeport++;
    
    if(DEBUG)
    {
        printf("add_child_node %s %s %s\n", ip, serverport, nodeport);
    }     

    for(j = 0; j < MAX_NODE_COUNT; j++)
    {
        if(pchildnodes[j] != NULL)
        {
            if( strcmp(pchildnodes[j]->ip, ip) == 0 && 
            	  atoi(serverport) == pchildnodes[j]->serverport ||
            	  atoi(serverport) == pchildnodes[j]->nodeport ||
            	  atoi(nodeport)   == pchildnodes[j]->serverport ||
            	  atoi(nodeport)   == pchildnodes[j]->nodeport )
            {
                break;
            }
        }
    }
    if(j < MAX_NODE_COUNT)
    {
        sprintf(outbuf, "error:Failed to add child node %s %s %s, node was already connected, or port was already used", ip, serverport, nodeport);
        send(server,outbuf, strlen(outbuf), 0);
        return NULL;
    }
    for(j = 0; j < MAX_NODE_COUNT; j++)
    {
        if(pchildnodes[j] == NULL)
        {
            pchildnodes[j] = (struct doom_dh_node*)malloc(sizeof(struct doom_dh_node));
            break;
        }
    }
                    
    if(j < MAX_NODE_COUNT)
    {
        strcpy(pchildnodes[j]->ip, ip);
        pchildnodes[j]->serverport    = atoi(serverport);
        pchildnodes[j]->nodeport      = atoi(nodeport);
        pchildnodes[j]->socket        = socket(AF_INET,SOCK_STREAM,0);
        pchildnodes[j]->id            = -1;
        pchildnodes[j]->firstcommand  = NULL;
        pchildnodes[j]->lastcommand   = NULL;
        
        nodeaddr.sin_family =AF_INET;  
        nodeaddr.sin_port   = htons(pchildnodes[j]->serverport);
        nodeaddr.sin_addr.s_addr=inet_addr(ip);
        ret = connect(pchildnodes[j]->socket,(struct sockaddr*)&nodeaddr,sizeof(nodeaddr));
        if(ret < 0)
        {
            free(pchildnodes[j]);
            pchildnodes[j] = NULL;
            sprintf(outbuf, "error:Failed to add child node, %s:%s\n", ip, serverport);
            send(server,outbuf, strlen(outbuf), 0);
            return NULL;
        }
        else
        {
            sprintf(outbuf, "info:New node added, %s:%s\n", ip, serverport);
            send(server,outbuf, strlen(outbuf), 0);
            return pchildnodes[j];
        }
    }
    else
    {
        sprintf(outbuf, "error:Failed to add child node, too many nodes");
        send(server,outbuf, strlen(outbuf), 0);
        return NULL;
    }
};

void handle_node_response(int index)
{
    int i, ret;
    struct doom_dh_client* pcli = NULL;
 		int t;
    char *str, *start, *end;
    struct doom_dh_command_list *pcommand;

    memset(inbuf, 0, BUFFER_SIZE);
    ret = recv(pchildnodes[index]->socket,inbuf,BUFFER_SIZE,0);
    if(DEBUG)
    {
        printf("handle_node_response: inbuf = \n%s\n", inbuf);
    }
    if(ret == 0)
    {   
        close(pchildnodes[index]->socket);
        printf("Node connection %s:%d closed!\n", pchildnodes[index]->ip, pchildnodes[index]->serverport);
        free(pchildnodes[index]);
        pchildnodes[index] = NULL;
    }
    else if(ret < 0)
    {
        printf("error recieve data, errno = %d\n", errno);
    }
    //ret > 0
    if(	pchildnodes[index]->firstcommand == 0 && //not waiting for input or result
	      strncasecmp(inbuf, "quit", strlen("quit")) == 0 )
	  {
        close(pchildnodes[index]->socket);
        printf("Connection %s:%d closed!\n", pchildnodes[index]->ip, pchildnodes[index]->serverport);
        free(pchildnodes[index]);
        pchildnodes[index] = NULL;
    }
    strcat(pchildnodes[index]->buffer, inbuf);
    if(DEBUG)
    {
        printf("handle_node_response: pchildnodes[%d]->buffer = \n%s\n", index, pchildnodes[index]->buffer);
    }
    
    while(1)
    {
		    str   = skip_space(pchildnodes[index]->buffer);
        //get one line
        start = end = str;
        while(*end != '\r' && *end != '\n' && *end != '\0')
        {
           end ++;
        }
        if(*end == '\0')
        {//line not finished
           break;
        }

        //get one line from "start" to "end"
        str = skip_space(end + 1);
        
        //trim space backward
        end = back_skip_space(end);
        *end = '\0';
        
        if(DEBUG)
        {
            printf("handle_node_response: start = %s\n", start);
        }
        
     	  pcommand = get_command(pchildnodes[index]->firstcommand->command.parentid, &pcli);
     	  assert(pcommand != NULL);

        if(strcasecmp(start, "/RES") ==  0)
        {   
            //the fisrt command of this node is finished, before removing it, we need handle its parent command
            
        	  //for parent command: waitingresult-- 
        	  assert(pcommand != NULL);
        	  pcommand->command.waitingresult--;
        	  if(pcommand->command.waitingresult == 0)
        	  {
        	      assert(pcommand->command.waitinginput == 0);

        	      //remove pcommand;
        	      if(pcommand->prev == NULL)
        	      {
        	          pcli->firstcommand = pcommand->next;
        	          if(pcli->firstcommand != NULL)
        	          {
        	              pcli->firstcommand->prev = NULL;
        	          }
        	      }
        	      else
        	      {
        	          pcommand->prev->next = pcommand->next;
        	          if(pcommand->next != NULL) //pcommand is not last
        	          {
                        pcommand->next->prev = pcommand->prev;
        	          }
        	          else
        	          {
        	              pcli->lastcommand = pcommand->prev;
        	          }
        	      }
  	            free(pcommand);
        	  }
        	  
        	  //remove fisrt command of this node
            assert(pchildnodes[index]->firstcommand != NULL);
            pcommand =pchildnodes[index]->firstcommand;
            pchildnodes[index]->firstcommand = pchildnodes[index]->firstcommand->next;
            if(pchildnodes[index]->firstcommand != NULL)
            {
                pchildnodes[index]->firstcommand->prev = NULL;
            }
            free(pcommand);
        }
        else if(strcasecmp(pchildnodes[index]->firstcommand->command.name, "get node info") == 0 &&
        	       strncasecmp(start, "ID:", strlen("ID:")) == 0 )
        {
            pchildnodes[index]->id = atoi(start + strlen("ID:"));
        }

        sprintf(outbuf, "%s(%s:%d)\n", start, pchildnodes[index]->ip, pchildnodes[index]->serverport);
        send(pcli->socket,outbuf, strlen(outbuf), 0);
        
        //handle one line
        i = 0;
        while(str[i] != '\0')
        {
            pchildnodes[index]->buffer[i] = str[i];
            i++;
        }
        pchildnodes[index]->buffer[i] = '\0';
    }
}

void handle_client_request(int index)
{
		int  j, t, ret;
    char *str, *start, *end;
    struct doom_dh_command_list *pcommand, *pchildcommand;    

    ret = recv(pclients[index]->socket,inbuf,BUFFER_SIZE,0);
    if(ret == 0)
    {   
        close(pclients[index]->socket);
        printf("Client connection %s:%d closed!\n", pclients[index]->ip, pclients[index]->port);
        free(pclients[index]);
        pclients[index] = NULL;
    }
    else if(ret < 0)
    {
        printf("error recieve data, errno = %d\n", errno);
    }
    //ret > 0
    if(DEBUG)
    {
        printf("handle_client_request inbuf = %s", inbuf);
    }

    if( pclients[index]->firstcommand == 0 && //not waiting for input/result
	      strncasecmp(inbuf, "quit", strlen("quit")) == 0 )
    {
        close(pclients[index]->socket);
        printf("Connection %s:%d closed!\n", pclients[index]->ip, pclients[index]->port);
        free(pclients[index]);
        pclients[index] = NULL;
    }
    strcat(pclients[index]->buffer, inbuf);
    
    while(1)
    {
        //get one line
        str   = skip_space(pclients[index]->buffer);
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
        
        if(DEBUG)
        {
            printf("handle_client_request: start = %s, client index = %d\n", start, index);
            dump_command_status(pclients[index]->firstcommand);
        }

        if(pclients[index]->lastcommand == NULL || //no command
        	 pclients[index]->lastcommand->command.waitinginput == 0) //finish input for last command
        {
            if(DEBUG)
            {
                printf("handle_client_request handle input start = %s\n", start);
            }
            if(strncasecmp(start, "MNG:", strlen("MNG:")) == 0)
            {
                //create a new command
                pcommand = (struct doom_dh_command_list *)malloc(sizeof(struct doom_dh_command_list));
                pcommand->command.id            = commandid++;
                pcommand->command.parentid      = -1; //this is already a parent command
                pcommand->command.waitinginput  = 0;
                pcommand->command.waitingresult = 0;
                pcommand->prev = pcommand->next = NULL;
                strcpy(pcommand->command.name, skip_space(start + strlen("MNG:")));
                
                if(strcasecmp(pcommand->command.name, "add child node") == 0)
                {
                    pcommand->command.waitinginput = 1;
                    for(j = 0; j < MAX_NODE_COUNT; j++)
                    {
                        if(pchildnodes[j] != NULL)
                        {
                            sprintf(outbuf, "MNG: add sibling node\n");
                            send(pchildnodes[j]->socket,outbuf, strlen(outbuf), 0);

                            pchildcommand = (struct doom_dh_command_list *)malloc(sizeof(struct doom_dh_command_list));
                            pchildcommand->command.id            = commandid++;
                            pchildcommand->command.parentid      = pcommand->command.id;
                            pchildcommand->command.waitinginput  = 1;
                            pchildcommand->command.waitingresult = 1;
                            pchildcommand->prev = pchildcommand->next = NULL;
                            strcpy(pchildcommand->command.name, "add sibling node");
                            
                            add_childcommand(pchildnodes[j], pchildcommand);
                            pcommand->command.waitingresult++;
                        }
                    }
                    add_command(pclients[index], pcommand);
                    
                }
                else if(strcasecmp(pcommand->command.name, "get node info") == 0)
                {
                    pcommand->command.waitinginput = 1;
                    for(j = 0; j < MAX_NODE_COUNT; j++)
                    {
                        if(pchildnodes[j] != NULL)
                        {
                            sprintf(outbuf, "MNG: %s\n", pcommand->command.name);
                            send(pchildnodes[j]->socket,outbuf, strlen(outbuf), 0);
                            
                            pchildcommand = (struct doom_dh_command_list *)malloc(sizeof(struct doom_dh_command_list));
                            pchildcommand->command.id            = commandid++;
                            pchildcommand->command.parentid      = pcommand->command.id;
                            pchildcommand->command.waitinginput  = 1;
                            pchildcommand->command.waitingresult = 1;
                            pchildcommand->prev = pchildcommand->next = NULL;
                            strcpy(pchildcommand->command.name, "get node info");
                            
                            add_childcommand(pchildnodes[j], pchildcommand);
                            pcommand->command.waitingresult++;
                        }
                    }
                    add_command(pclients[index], pcommand);
                }
                else
                {
                    printf("error MNG command %s\n", pcommand->command.name);
                    free(pcommand);
                }
            }
            else if(strncasecmp(start, "TSK:", strlen("TSK:")) == 0)
            {
                //create a new command
                pcommand = (struct doom_dh_command_list *)malloc(sizeof(struct doom_dh_command_list));
                pcommand->command.id            = commandid++;
                pcommand->command.parentid      = -1; //this is already a parent command
                pcommand->command.waitinginput  = 0;
                pcommand->command.waitingresult = 0;
                pcommand->prev = pcommand->next = NULL;
                strcpy(pcommand->command.name, skip_space(start + strlen("TSK:")));
                
                if(DEBUG)
                {
                    printf("handle_client_request handle TSK command = %s\n", pcommand->command.name);
                }
                if(strcasecmp(pcommand->command.name, "create database") == 0 ||
                   strcasecmp(pcommand->command.name, "set database")    == 0 ||
                   strcasecmp(pcommand->command.name, "execute ddl")     == 0 ||
                	 strcasecmp(pcommand->command.name, "execute dml")     == 0 ||
                	 strcasecmp(pcommand->command.name, "execute dql")     == 0)
                {
                    pcommand->command.waitinginput = 1;
                    for(j = 0; j < MAX_NODE_COUNT; j++)
                    {
                        if(pchildnodes[j] != NULL)
                        {
                            sprintf(outbuf, "TSK: %s\n", pcommand->command.name);
                            send(pchildnodes[j]->socket,outbuf, strlen(outbuf), 0);

                            pchildcommand = (struct doom_dh_command_list *)malloc(sizeof(struct doom_dh_command_list));
                            pchildcommand->command.id            = commandid++;
                            pchildcommand->command.parentid      = pcommand->command.id;
                            pchildcommand->command.waitinginput  = 1;
                            pchildcommand->command.waitingresult = 1;
                            pchildcommand->prev = pchildcommand->next = NULL;
                            strcpy(pchildcommand->command.name, pcommand->command.name);

                            add_childcommand(pchildnodes[j], pchildcommand);
                            pcommand->command.waitingresult++;
                        }
                    }
                    add_command(pclients[index], pcommand);
                }
                else
                {
                    printf("error TSK command %s\n", pcommand->command.name);
                    free(pcommand);
                }
            }
            else
            {
                printf("error request %s\n", start);
            }
        }
        else if(pclients[index]->lastcommand != NULL && pclients[index]->lastcommand->command.waitinginput == 1) //waiting for input
        {
            if(strcasecmp(pclients[index]->lastcommand->command.name, "add child node") == 0)	
            {
                struct doom_dh_node* pnode;
                if(strcasecmp(start, "/MNG") != 0)
                {
                    if((pnode = add_child_node(start, pclients[index]->socket))!=NULL)
                    {
                        for(j = 0; j < MAX_NODE_COUNT; j++)
                        {
                            if(pchildnodes[j] != NULL)
                            {
                                if(pchildnodes[j] != pnode)
                                {//add sibling node
                                    sprintf(outbuf, "%s:%d\n", pnode->ip, pnode->nodeport);
                                    send(pchildnodes[j]->socket,outbuf, strlen(outbuf), 0);
                                }
                                else
                                {
                                    //start add sibling node
                                    sprintf(outbuf, "MNG: add sibling node\n");
                                    send(pchildnodes[j]->socket,outbuf, strlen(outbuf), 0);


                                    pchildcommand = (struct doom_dh_command_list *)malloc(sizeof(struct doom_dh_command_list));
                                    pchildcommand->command.id            = commandid++;
                                    pchildcommand->command.parentid      = pclients[index]->lastcommand->command.id;
                                    pchildcommand->command.waitinginput  = 1;
                                    pchildcommand->command.waitingresult = 1;
                                    pchildcommand->prev = pchildcommand->next = NULL;
                                    strcpy(pchildcommand->command.name, "add sibling node");
                            
                                    add_childcommand(pchildnodes[j], pchildcommand);
                                    pcommand->command.waitingresult++;

                                }
                            }
                        }
                    }
                }
                else
                {
                    sprintf(outbuf, "/MNG\n");
                    for(j = 0; j < MAX_NODE_COUNT; j++)
                    {
                        if(pchildnodes[j] != NULL)
                        {
                            send(pchildnodes[j]->socket,outbuf, strlen(outbuf), 0);
                            pchildnodes[j]->lastcommand->command.waitinginput = 0;
                        }
                    }
                    pclients[index]->lastcommand->command.waitinginput = 0; //no need to wait for input
                }
            }
            else if(strcasecmp(pclients[index]->lastcommand->command.name, "create database") == 0 ||
            	       strcasecmp(pclients[index]->lastcommand->command.name, "get node info")   == 0 ||
                     strcasecmp(pclients[index]->lastcommand->command.name, "set database")    == 0 ||
                     strcasecmp(pclients[index]->lastcommand->command.name, "execute ddl")     == 0 ||
                	   strcasecmp(pclients[index]->lastcommand->command.name, "execute dml")     == 0 ||
                	   strcasecmp(pclients[index]->lastcommand->command.name, "execute dql")     == 0)
            {
                if(strcasecmp(start, "/TSK") == 0 || strcasecmp(start, "/MNG") == 0)
                {
                    pclients[index]->lastcommand->command.waitinginput = 0; //no need to wait for input
                }
                int id = -1;
                if(*start == '#')
                {
                    id = atoi(start + 1);
                    while(*start != ':')
                    {
                        start++;
                    }
                    start ++;
                }
                for(j = 0; j < MAX_NODE_COUNT; j++)
                {
                    if(pchildnodes[j] != NULL && (id == -1 || id == pchildnodes[j]->id))
                    {
                        sprintf(outbuf, "%s\n", start);
                        send(pchildnodes[j]->socket,outbuf, strlen(outbuf), 0);
                        if(strcasecmp(start, "/TSK") == 0 || strcasecmp(start, "/MNG") == 0)
                        {
                            pchildnodes[j]->lastcommand->command.waitinginput = 0; //no need to wait for input
                        }
                    }
                }
            }
            else
            {
                printf("error request %s, command=%s\n", start, pclients[index]->lastcommand->command.name);
            }
        }
        else
        {
            printf("server is not ready to handle request!\n");
            dump_command_status(pclients[index]->firstcommand);
        }

        j = 0;
        while(str[j] != '\0')
        {
            pclients[index]->buffer[j] = str[j];
            j++;
        }
        pclients[index]->buffer[j] = '\0';
    }
    
}

int main(int argc, char* argv[])  
{
    int    i, j, ret;
    int    ser, cli;
    int    port;
    
    struct sockaddr_in seraddr,cliaddr;  
    fd_set  rdfs;

    if(argc != 3)
    {
        printf("Usage: %s IP PORT\n", argv[0]);
        return 1;
    }
    
    port = atoi(argv[2]);
    
    socklen_t clilen        =sizeof(cliaddr);  
 
    seraddr.sin_family      = AF_INET;  
    //seraddr.sin_addr.s_addr = htonl(INADDR_ANY);  
    seraddr.sin_addr.s_addr = inet_addr(argv[1]);
    seraddr.sin_port        = htons(port);  
    ser    = socket(AF_INET,SOCK_STREAM,0);  
    ret    = bind(ser,(struct sockaddr*)&seraddr,sizeof(seraddr));
    
    ret    = listen(ser,5); 
    if(ret < 0)
    {
        printf("Failed to listen on port %d, errno = %d\n", port, errno);
        return 1;
    }
    printf("Listening on %s:%s...\n", argv[1], argv[2]);
    
    for( i = 0; i < MAX_NODE_COUNT; i++)
    {
        pchildnodes[i] = NULL;
    }
    for( i = 0; i < MAX_CLIENT_COUNT; i++)
    {
        pclients[i] = NULL;
    }

    while(1)  
    {   
        int max_fd = -1;
        FD_ZERO(&rdfs);
        FD_SET(ser, &rdfs);
        max_fd = ser;
        //dump client and node socket status
        if(DEBUG)
        {
            printf("=============Loop started===============\n"	);
            printf("\nclient socket status:\n");
            for(i = 0; i < MAX_CLIENT_COUNT; i++)
            {
                if(pclients[i] != NULL)
                {
                    printf("client[%d](%s:%d)\n", i, pclients[i]->ip, pclients[i]->port);
                    dump_command_status(pclients[i]->firstcommand);
                }
            }
            printf("\nchild node socket status:\n");
            for(j = 0; j < MAX_NODE_COUNT; j++)
            {
                if(pchildnodes[j] != NULL)
                {
                    printf("childnode[%d](%s:%d)\n", j, pchildnodes[j]->ip, pchildnodes[j]->serverport);
                    dump_command_status(pchildnodes[j]->firstcommand);
                }
            }
        }
        //dump end
        
        for(i = 0; i < MAX_CLIENT_COUNT; i++)
        {
            if(pclients[i] != NULL)
            {
                FD_SET(pclients[i]->socket, &rdfs);
                max_fd = pclients[i]->socket > max_fd ? pclients[i]->socket : max_fd;
            }
        }
        for(i = 0; i < MAX_NODE_COUNT; i++)
        {
            if(pchildnodes[i] != NULL && pchildnodes[i]->firstcommand != NULL) //only receive the msg from nodes who are waiting for result. 
            {
                FD_SET(pchildnodes[i]->socket, &rdfs);
                max_fd = pchildnodes[i]->socket > max_fd ? pchildnodes[i]->socket : max_fd;
                if(DEBUG)
                {
                    printf("select from childnode[%d]\n", i);
                }
            }
        }
        ret = select(max_fd + 1,&rdfs,NULL, NULL, NULL);
        
        //dump select result
        if(DEBUG)
        {
            printf("\nselect result:\n");
            for(i = 0; i < MAX_CLIENT_COUNT; i++)
            {
                if(pclients[i]!= NULL && FD_ISSET(pclients[i]->socket, &rdfs))
                {
                    printf("client[%d] is selected\n", i);;
                }
            }
            for(j = 0; j < MAX_NODE_COUNT; j++)
            {
                if(pchildnodes[j] != NULL && FD_ISSET(pchildnodes[j]->socket, &rdfs))
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
                    for(i = 0; i < MAX_CLIENT_COUNT; i++)
                    {
                        if(pclients[i] == NULL)
                        {
                            pclients[i] = (struct doom_dh_client*)malloc(sizeof(struct doom_dh_client));
                            pclients[i]->socket        = cli;
                            strcpy(pclients[i]->ip,inet_ntoa(cliaddr.sin_addr));
                            pclients[i]->port          = ntohs(cliaddr.sin_port);
                            pclients[i]->firstcommand  = NULL;
                            pclients[i]->lastcommand   = NULL;
                            pclients[i]->buffer[0]     = '\0';
                            break;
                        }
                    }
                    if(i == MAX_CLIENT_COUNT)
                    {
                        sprintf(outbuf, "ERR: failed to connect to the server, too many connections\n");
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
            else        
            {
                for(i = 0; i < MAX_CLIENT_COUNT; i++)
                {
                    if(pclients[i]!= NULL && FD_ISSET(pclients[i]->socket, &rdfs))
                    {
                        handle_client_request(i);	
                    }
                }

                for(j = 0; j < MAX_NODE_COUNT; j++)
                {
                    if(pchildnodes[j] != NULL && FD_ISSET(pchildnodes[j]->socket, &rdfs))
                    {
                        handle_node_response(j);
                    }
                }
            }
        }
    }
    return 1;  
}