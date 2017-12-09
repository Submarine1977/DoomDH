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

struct doom_dh_node
{
    int   id;	
    int   socket;
    char  ip[16];
    int   serverport;   
    int   nodeport;
    
    char  command[32];
    int   waitinginput;
    int   waitingresult;
    char  buffer[BUFFER_SIZE*2];
};
struct    doom_dh_node*  pchildnodes[MAX_NODE_COUNT];

struct doom_dh_client
{
    int   socket;
    char  ip[16];
    int   port;
    
    char  command[32];
    int   waitinginput;
    int   waitingresult;
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

struct doom_dh_node* add_child_node(char* buffer, int server)
{
    int j, ret;
    char *ip;
    char *serverport, *nodeport;
    struct sockaddr_in nodeaddr;  

    ip = buffer;
    while(*ip == ' ') //skip ' '
        ip++;

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
        pchildnodes[j]->waitinginput  = 0;
        pchildnodes[j]->waitingresult = 0;
        

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
    if(	pchildnodes[index]->waitinginput <= 0 && //not waiting for input
	      strncasecmp(inbuf, "quit", strlen("quit")) == 0 )
	  {
        close(pchildnodes[index]->socket);
        printf("Connection %s:%d closed!\n", pchildnodes[index]->ip, pchildnodes[index]->serverport);
        free(pchildnodes[index]);
        pchildnodes[index] = NULL;
    }
    strcat(pchildnodes[index]->buffer, inbuf);
    
    //get the client who is waiting for the result;
    //LIMITATION: ONLY ONE client is waiting for result!!!!
    for(i = 0; i < MAX_CLIENT_COUNT; i++)
    {
        if(pclients[i] != NULL && pclients[i]->waitingresult > 0)
        {
            pcli = pclients[i];
            break;
        }
    }
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
        
        if(strcasecmp(start, "/RES") ==  0)
        {
            pchildnodes[index]->waitingresult--; //not waiting for result
        }
        
        if(strcasecmp(pchildnodes[index]->command, "get node info") == 0)
        {
            pchildnodes[index]->id = atoi(start);
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
    
    
    int waiting_result = 0;
    for(i = 0; i < MAX_NODE_COUNT; i++)
    {
        if(pchildnodes[i])
        {
            waiting_result = waiting_result > pchildnodes[i]->waitingresult ? waiting_result : pchildnodes[i]->waitingresult;
        }
    }
    
    if(DEBUG)
    {
        printf("handle_node_response: waiting_result = %d\n", waiting_result);
    }
    
    if(waiting_result == 0 && pcli != NULL)
    {
        pcli->waitingresult = waiting_result;
    }
}

void handle_client_request(int index)
{
		int  j, t, ret;
    char *str, *start, *end;
    int  flag; //if there is request send to node and need wait for feedback.
    

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

    if( pclients[index]->waitinginput <= 0 && //not waiting for input
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
            printf("handle_client_request: start = %s, pclients[index]->waitingresult = %d, pclients[index]->waitinginput = %d\n", start, pclients[index]->waitingresult, pclients[index]->waitinginput);
        }

        if(pclients[index]->waitinginput == 0) //waiting input for new request
        {
            if(DEBUG)
            {
                printf("handle_client_request handle input start = %s\n", start);
            }
            if(strncasecmp(start, "MNG:", strlen("MNG:")) == 0)
            {
                strcpy(pclients[index]->command, skip_space(start + strlen("MNG:")));
                if(strcasecmp(pclients[index]->command, "add child node") == 0)
                {
                    pclients[index]->waitinginput = 1;
                    flag = 0;
                    for(j = 0; j < MAX_NODE_COUNT; j++)
                    {
                        if(pchildnodes[j] != NULL)
                        {
                            sprintf(outbuf, "MNG: add sibling node\n");
                            send(pchildnodes[j]->socket,outbuf, strlen(outbuf), 0);
                            strcpy(pchildnodes[j]->command, "add sibling node");
                            pchildnodes[j]->waitingresult++;
                            pclients[index]->waitingresult++;
                            flag = 1;
                        }
                    }
                    if(flag)
                    {
                        pclients[index]->waitingresult++;
                    }
                }
                else if(strcasecmp(pclients[index]->command, "get node info") == 0)
                {
                    pclients[index]->waitinginput = 1;
                    flag = 0;
                    for(j = 0; j < MAX_NODE_COUNT; j++)
                    {
                        if(pchildnodes[j] != NULL)
                        {
                            sprintf(outbuf, "MNG: %s\n", pclients[index]->command);
                            send(pchildnodes[j]->socket,outbuf, strlen(outbuf), 0);
                            strcpy(pchildnodes[j]->command, pclients[index]->command);
                            pchildnodes[j]->waitingresult++;
                            flag = 1;
                        }
                    }
                    if(flag)
                    {
                        pclients[index]->waitingresult++;
                    }
                }
                else
                {
                    printf("error MNG command %s\n", pclients[index]->command);
                }
            }
            else if(strncasecmp(start, "TSK:", strlen("TSK:")) == 0)
            {
                strcpy(pclients[index]->command, skip_space(start + strlen("TSK:")));
                if(DEBUG)
                {
                    printf("handle_client_request handle TSK command = %s\n", pclients[index]->command);
                }
                if(strcasecmp(pclients[index]->command, "create database") == 0 ||
                   strcasecmp(pclients[index]->command, "set database")    == 0 ||
                   strcasecmp(pclients[index]->command, "execute ddl")     == 0 ||
                	 strcasecmp(pclients[index]->command, "execute dml")     == 0 ||
                	 strcasecmp(pclients[index]->command, "execute dql")     == 0)
                {
                    pclients[index]->waitinginput = 1;
                    flag = 0;
                    for(j = 0; j < MAX_NODE_COUNT; j++)
                    {
                        if(pchildnodes[j] != NULL)
                        {
                            sprintf(outbuf, "TSK: %s\n", pclients[index]->command);
                            send(pchildnodes[j]->socket,outbuf, strlen(outbuf), 0);
                            strcpy(pchildnodes[j]->command, pclients[index]->command);
                            pchildnodes[j]->waitingresult++;
                            flag = 1;
                        }
                    }
                    if(DEBUG)
                    {
                        printf("handle_client_request handle TSK flag = %d\n", flag);
                    }
                    if(flag)
                    {
                        pclients[index]->waitingresult++;
                    }
                }
                else
                {
                    printf("error TSK command %s\n", pclients[index]->command);
                }
            }
            else
            {
                printf("error request %s\n", start);
            }
        }
        else if(pclients[index]->waitinginput == 1) //waiting for input
        {
            if(strcasecmp(pclients[index]->command, "add child node") == 0)	
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
                                    strcpy(pchildnodes[j]->command, "add sibling node");
                                    pchildnodes[j]->waitingresult++;
                                    if(flag == 0)
                                    {
                                       flag = 1;	
                                       pclients[index]->waitingresult++;
                                    }
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
                        }
                    }
                    pclients[index]->waitinginput = 0; //no need to wait for input
                }
            }
            else if(strcasecmp(pclients[index]->command, "create database") == 0 ||
            	       strcasecmp(pclients[index]->command, "get node info")   == 0 ||
                     strcasecmp(pclients[index]->command, "set database")    == 0 ||
                     strcasecmp(pclients[index]->command, "execute ddl")     == 0 ||
                	   strcasecmp(pclients[index]->command, "execute dml")     == 0 ||
                	   strcasecmp(pclients[index]->command, "execute dql")     == 0)
            {
                if(strcasecmp(start, "/TSK") == 0 || strcasecmp(start, "/MNG") == 0)
                {
                    pclients[index]->waitinginput = 0; //no need to wait for input
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
                    }
                }
            }
            else
            {
                printf("error request %s, command=%s\n", start, pclients[index]->command);
            }
        }
        else
        {
            printf("server is not ready to handle request waitinginput=%d, waitingresult=%d, request=%s\n", 
                    pclients[index]->waitinginput, pclients[index]->waitingresult, start);
        }

        //strcpy(pclients[index]->buffer, str); //handle one line
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
                    printf("client[%d](%s:%d),waitinginput=%d, waitingresult=%d\n", i, pclients[i]->ip, pclients[i]->port, pclients[i]->waitinginput, pclients[i]->waitingresult);
                }
            }
            printf("\nchild node socket status:\n");
            for(j = 0; j < MAX_NODE_COUNT; j++)
            {
                if(pchildnodes[j] != NULL)
                {
                    printf("childnode[%d](%s:%d),waitinginput=%d, waitingresult=%d\n", j, pchildnodes[j]->ip, pchildnodes[j]->serverport, pchildnodes[j]->waitinginput, pchildnodes[j]->waitingresult);
                }
            }
        }
        //dump end
        
        //check if there is client waiting for input or result
        struct doom_dh_client *pcli = NULL;
        for(i = 0; i < MAX_CLIENT_COUNT; i++)
        {
            if(pclients[i] != NULL && (pclients[i]->waitinginput > 0 || pclients[i]->waitingresult > 0) )
            {
                pcli = pclients[i];
                break;
            }
        }
        if(pcli == NULL) //there is no client waiting for input or result, receive msg from all client
        {	
            for(i = 0; i < MAX_CLIENT_COUNT; i++)
            {
                if(pclients[i] != NULL)
                {
                    FD_SET(pclients[i]->socket, &rdfs);
                    max_fd = pclients[i]->socket > max_fd ? pclients[i]->socket : max_fd;
                    if(DEBUG)
                    {
                        printf("select from client[%d]\n", i);
                    }
                }
            }
        }
        else // there is one client who is waiting for input or result.
        {
            if(pclients[i]->waitinginput > 0) //receive msg from this client when it is also waiting for input
            {
                FD_SET(pcli->socket, &rdfs);
                max_fd = pcli->socket > max_fd ? pcli->socket : max_fd;
                if(DEBUG)
                {
                    printf("select from client[%d]\n", i);
                }
            }
        }
        for(i = 0; i < MAX_NODE_COUNT; i++)
        {
            if(pchildnodes[i] != NULL && pchildnodes[i]->waitingresult > 0) //only receive the msg from nodes who are waiting for result. 
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
                            pclients[i]->socket    = cli;
                            strcpy(pclients[i]->ip,inet_ntoa(cliaddr.sin_addr));
                            pclients[i]->port = ntohs(cliaddr.sin_port);
                            pclients[i]->command[0]    = '\0';
                            pclients[i]->waitinginput  = 0;
                            pclients[i]->waitingresult = 0;
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