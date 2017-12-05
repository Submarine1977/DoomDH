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

#define SIZE 1024
#define MAX_NODE 1024
                                      
int    fds[3];
char   fdsip[16][3];
int    fdsport[3];
const int len = 3;
char   inbuf[SIZE];
char   outbuf[SIZE];  

struct doom_dh_node
{
    int   socket;
    char  ip[16];
    int   port;   
};
struct    doom_dh_node*  psiblingnodes[MAX_NODE];

struct doom_dh_node* add_sibling_node(int server)
{
    int j, ret;
    char* ip;
    char* port;
    struct sockaddr_in nodeaddr;  
                    
    ip  = inbuf + strlen("addsiblingnode");
    while(*ip == ' ') 
        ip++;
    port = ip;
    while(*port != ':') port++;
    *port = '\0';
    port ++;
    
    for(j = 0; j < MAX_NODE; j++)
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
    if(j < MAX_NODE)
    {
        sprintf(outbuf, "ERR:Failed to add sibling node %s %s, node was already connected, or port was already used", ip, port);
        send(server,outbuf, strlen(outbuf), 0);
        return NULL;
    }
    
                    
    for(j = 0; j < MAX_NODE; j++)
    {
        if(psiblingnodes[j] == NULL)
        {
            psiblingnodes[j] = (struct doom_dh_node*)malloc(sizeof(struct doom_dh_node));
            break;
        }
    }
                    
    if(j < MAX_NODE)
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
            sprintf(outbuf, "ERR:Failed to add sibling node, %s:%s, errno=%d\n", ip, port, errno);
            send(server,outbuf, strlen(outbuf), 0);
            return NULL;
        }
        else
        {
            sprintf(outbuf, "INF:New sibling node added, %s:%s\n", ip, port);
            send(server,outbuf, strlen(outbuf), 0);
            return psiblingnodes[j];
        }
        
    }
    else
    {
        sprintf(outbuf, "ERR:Failed to add sibling node, too many nodes\n");
        send(server,outbuf, strlen(outbuf), 0);
        return NULL;
    }
};



int main(int argc, char* argv[])  
{
    int    i, j, ret;
    int    ser,node,cli;  
    struct sockaddr_in seraddr,nodeaddr, cliaddr;  
    fd_set  rdfs;
    int     serverport, nodeport;
    
    if(argc != 4)
    {
        printf("Usage: %s IP SERVER_PORT NODE_PORT\n", argv[0]);
        return 1;
    }
    
    
    serverport = atoi(argv[2]);
    nodeport   = atoi(argv[3]);

    socklen_t clilen        = sizeof(cliaddr);  
 
    seraddr.sin_family      = AF_INET;  
    //seraddr.sin_addr.s_addr = htonl(INADDR_ANY);  
    seraddr.sin_addr.s_addr = inet_addr(argv[1]);
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
    
    printf("Listening server on %s:%s...\n", argv[1], argv[2]);
    printf("Listening node on %s:%s...\n", argv[1], argv[3]);

    
    for( i = 0; i < MAX_NODE; i++)
    {
        psiblingnodes[i] = NULL;
    }
    for(i = 0; i < len; i++)
    {
        fds[i] = -1;
    }
    fds[0] = ser;
    fds[1] = node;

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
            max_fd = fds[i] > max_fd ? fds[i] : max_fd;
        }
        for(i = 0; i < MAX_NODE; i++)
        {
            if(psiblingnodes[i] != NULL)
            {
                FD_SET(psiblingnodes[i]->socket, &rdfs);
                max_fd = psiblingnodes[i]->socket > max_fd ? psiblingnodes[i]->socket : max_fd;
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
                        sprintf(outbuf, "ERR:The node was already connected to a server.\n");
                        send(cli,outbuf, strlen(outbuf), 0);
                        close(cli);
                    }
                    else
                    {
                        printf("connect to server %s:%d\n",inet_ntoa(cliaddr.sin_addr),  
                                ntohs(cliaddr.sin_port));
                    }
                }
            }
            else if(FD_ISSET(node, &rdfs))
            {
                cli=accept(node,(struct sockaddr*)&cliaddr,&clilen);
                if(cli > 0)
                {
                    for(i = 0; i < MAX_NODE; i++)
                    {
                        if(psiblingnodes[i] == NULL)
                        {
                            psiblingnodes[i] = (struct doom_dh_node*)malloc(sizeof(struct doom_dh_node));
                            psiblingnodes[i]->socket = cli;
                            strcpy(psiblingnodes[i]->ip, inet_ntoa(cliaddr.sin_addr));
                            psiblingnodes[i]->port = ntohs(cliaddr.sin_port);
                            break;
                        }
                    }
                    if(i == MAX_NODE)
                    {
                        sprintf(outbuf, "ERR:Failed to connect to sibling node, too many connections\n");
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
                        if(psiblingnodes[j] != NULL && FD_ISSET(psiblingnodes[j]->socket, &rdfs))
                        {
                            cli = psiblingnodes[j]->socket;
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
            	          printf("Connection %s:%d closed!\n", psiblingnodes[j]->ip, psiblingnodes[j]->port);
            	          assert(j < MAX_NODE);
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
                //ret > 0
                if(strcmp(inbuf, "quit\r\n") == 0)
                {
            	      close(cli);
            	      if(i < len)
            	      {
            	          printf("Connection %s:%d closed!\n", fdsip[i], fdsport[i]);
            	          fds[i] = -1;
            	      }
            	      else
            	      {
            	          printf("Connection %s:%d closed!\n", psiblingnodes[j]->ip, psiblingnodes[j]->port);
            	          assert(j < MAX_NODE);
            	          free(psiblingnodes[j]);
            	          psiblingnodes[j] = NULL;
            	      }
            	      continue;
                }

                if(strncmp(inbuf, "addsiblingnode", strlen("addsiblingnode")) == 0)
                {
                    struct doom_dh_node* pnode;
                    pnode = add_sibling_node(cli);
                    continue;
                }

                j = strlen(inbuf);
                while(j > 0 && (inbuf[j - 1] == '\r' || inbuf[j - 1] == '\n'))
                {
                    inbuf[j - 1] = '\0';
                    j--;
                }
                printf("%s\n", inbuf);
                if(strncmp(inbuf,"TSK:", strlen("TSK:")) == 0 && strstr(inbuf, "node") == NULL)
                {
                    sprintf(outbuf, "%s(node: %s:%d)\n", inbuf, inet_ntoa(seraddr.sin_addr), ntohs(seraddr.sin_port));
                    for(j = 0; j < MAX_NODE; j++)
                    {
                        if(psiblingnodes[j])
                        {
                            send(psiblingnodes[j]->socket,outbuf, strlen(outbuf), 0);
                        }
                    }
                }
            }
        }
    }
    return 1;  
}