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

#define SIZE     1024
#define MAX_NODE 1024

int       fds[5];
char      fdsip[16][5];
int       fdsport[5];
const int len = 5;
char      inbuf[SIZE];
char      outbuf[SIZE];  

struct doom_dh_node
{
    int   socket;
    char  ip[16];
    int   serverport;   
    int   nodeport;
};
struct    doom_dh_node*  pchildnodes[MAX_NODE];

struct doom_dh_node* add_child_node(int server)
{
    int j, ret;
    char *ip;
    char *serverport, *nodeport;
    struct sockaddr_in nodeaddr;  

    ip = inbuf + strlen("addchildnode");
    while(*ip == ' ') //skip ' '
        ip++;

    serverport = ip;
    while(*serverport != ' ') 
        serverport++;
    *serverport = '\0';
    serverport++;
    
    nodeport = serverport + 1;
    while(*nodeport != ' ') 
        nodeport++;
    *nodeport = '\0';
    nodeport++;
                    

    for(j = 0; j < MAX_NODE; j++)
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
    if(j < MAX_NODE)
    {
        sprintf(outbuf, "ERR:Failed to add child node %s %s %s, node was already connected, or port was already used", ip, serverport, nodeport);
        send(server,outbuf, strlen(outbuf), 0);
        return NULL;
    }
    for(j = 0; j < MAX_NODE; j++)
    {
        if(pchildnodes[j] == NULL)
        {
            pchildnodes[j] = (struct doom_dh_node*)malloc(sizeof(struct doom_dh_node));
            break;
        }
    }
                    
    if(j < MAX_NODE)
    {
        strcpy(pchildnodes[j]->ip, ip);
        pchildnodes[j]->serverport = atoi(serverport);
        pchildnodes[j]->nodeport   = atoi(nodeport);
        pchildnodes[j]->socket = socket(AF_INET,SOCK_STREAM,0);

        nodeaddr.sin_family =AF_INET;  
        nodeaddr.sin_port   = htons(pchildnodes[j]->serverport);
        nodeaddr.sin_addr.s_addr=inet_addr(ip);
        ret = connect(pchildnodes[j]->socket,(struct sockaddr*)&nodeaddr,sizeof(nodeaddr));
        if(ret < 0)
        {
            free(pchildnodes[j]);
            pchildnodes[j] = NULL;
            sprintf(outbuf, "ERR:Failed to add child node, %s:%s\n", ip, serverport);
            send(server,outbuf, strlen(outbuf), 0);
            return NULL;
        }
        else
        {
            sprintf(outbuf, "INF:New node added, %s:%s\n", ip, serverport);
            send(server,outbuf, strlen(outbuf), 0);
            return pchildnodes[j];
        }
        
    }
    else
    {
        sprintf(outbuf, "ERR:Failed to add child node, too many nodes");
        send(server,outbuf, strlen(outbuf), 0);
        return NULL;
    }
};

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
    
    for( i = 0; i < MAX_NODE; i++)
    {
        pchildnodes[i] = NULL;
    }
    for(i = 0; i < len; i++)
    {
        fds[i] = -1;
    }
    fds[0] = ser;

    while(1)  
    {   
        int max_fd = -1;
        FD_ZERO(&rdfs);
        for(i = 0; i < len; i++)
        {
            if(fds[i] != -1)
            {
                FD_SET(fds[i], &rdfs);
            }
            max_fd = fds[i] > max_fd?fds[i]:max_fd;
        }
        for(i = 0; i < MAX_NODE; i++)
        {
            if(pchildnodes[i] != NULL)
            {
                FD_SET(pchildnodes[i]->socket, &rdfs);
                max_fd = pchildnodes[i]->socket > max_fd ? pchildnodes[i]->socket : max_fd;
            }
        }
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
            memset(inbuf,0,SIZE);
            memset(outbuf, 0, SIZE);
            if(FD_ISSET(ser, &rdfs))  
            {  
                cli=accept(ser,(struct sockaddr*)&cliaddr,&clilen);
                if(cli > 0)
                {
                    for(i = 0; i < len; i++)
                    {
                        if(fds[i] == -1)
                        {
                            fds[i] = cli;
                            strcpy(fdsip[i],inet_ntoa(cliaddr.sin_addr));
                            fdsport[i] = ntohs(cliaddr.sin_port);
                            break;
                        }
                    }
                    if(i == len)
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
                for(i = 0; i < len; i++)
                {
                    if(i != 0 && FD_ISSET(fds[i], &rdfs))
                    {
                        cli = fds[i];	
                        break;
                    }
                }
                if(i >= len)
                {
                    for(j = 0; j < MAX_NODE; j++)
                    {
                        if(pchildnodes[j] != NULL && FD_ISSET(pchildnodes[j]->socket, &rdfs))
                        {
                            cli = pchildnodes[j]->socket;
                            break;
                        }
                    }
                    assert(j < MAX_NODE);
                }
                ret = recv(cli,inbuf,SIZE,0);
            	  if(ret == 0)
            	  {   
            	      close(cli);
            	      if(i < len)
            	      {
            	          printf("Connection %s:%d closed!\n", fdsip[i], fdsport[i]);
            	          fds[i] = -1;
            	      }
            	      else
            	      {
            	          printf("Connection %s:%d closed!\n", pchildnodes[j]->ip, pchildnodes[j]->serverport);
            	          assert(j < MAX_NODE);
            	          free(pchildnodes[j]);
            	          pchildnodes[j] = NULL;
            	      }
            	      continue;
            	  }
            	  else if(ret < 0)
            	  {
            	      printf("error recieve data, errno = %d\n", errno);
            	      continue;
                }
                //ret > 0
                if(strncmp(inbuf, "quit", 4) == 0)
                {
            	      close(cli);
                    if(i < len)
                    {
            	          printf("Connection %s:%d closed!\n", fdsip[i], fdsport[i]);
                        fds[i] = -1;
                    }
                    else
                    {
            	          printf("Connection %s:%d closed!\n", pchildnodes[j]->ip, pchildnodes[j]->serverport);
            	          assert(j < MAX_NODE);
            	          free(pchildnodes[j]);
            	          pchildnodes[j] = NULL;
                    }
            	      continue;
                }
                
                if(strncmp(inbuf, "addchildnode", strlen("addchildnode")) == 0)
                {
                    struct doom_dh_node* pnode;
                    if((pnode = add_child_node(cli))!=NULL)
                    {
                        for(j = 0; j < MAX_NODE; j++)
                        {
                            if(pchildnodes[j] != NULL && pchildnodes[j] != pnode)
                            {
                                sprintf(outbuf, "addsiblingnode %s:%d\n", pnode->ip, pnode->nodeport);
                                send(pchildnodes[j]->socket,outbuf, strlen(outbuf), 0);
                            }
                        }
                    }
                    continue;
                }
                
                j = strlen(inbuf);
                while(j > 0 && (inbuf[j - 1] == '\r' || inbuf[j - 1] == '\n'))
                {
                    inbuf[j - 1] = '\0';
                    j--;
                }
                printf("%s\n", inbuf);
                if(strncmp(inbuf,"TSK:", strlen("TSK:")) == 0)
                {
                    sprintf(outbuf, "%s(server: %s:%d)\n", inbuf, inet_ntoa(seraddr.sin_addr), ntohs(seraddr.sin_port));
                    for(j = 0; j < MAX_NODE; j++)
                    {
                        if(pchildnodes[j])
                        {
                            send(pchildnodes[j]->socket,outbuf, strlen(outbuf), 0);
                        }
                    }
                }
            }
        }
    }
    return 1;  
}