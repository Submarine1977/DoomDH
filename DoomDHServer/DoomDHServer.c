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
#define PORT     8274
#define MAX_NODE 1024

int    fds[5];
const int len = 5;

struct mood_dh_node
{
    int   socket;
    char  ip[16];
    int   port;   
};

int main()  
{
    int    i, j, ret;
    int    ser, cli;
    
    char   inbuf[SIZE];
    char   outbuf[SIZE];  
    struct sockaddr_in seraddr,cliaddr, nodeaddr;  
    fd_set  rdfs;

    struct mood_dh_node*  pnode[MAX_NODE];
    
    
    socklen_t clilen        =sizeof(cliaddr);  
 
    seraddr.sin_family      = AF_INET;  
    seraddr.sin_addr.s_addr = htonl(INADDR_ANY);  
    seraddr.sin_port        = htons(PORT);  
    ser    = socket(AF_INET,SOCK_STREAM,0);  
    ret    = bind(ser,(struct sockaddr*)&seraddr,sizeof(seraddr));
    
    for( i = 0; i < MAX_NODE; i++)
    {
        pnode[i] = NULL;
    }

    ret    = listen(ser,5); 
    if(ret < 0)
    {
        printf("Failed to listen on port %d, errno = %d\n", PORT, errno);
        return 1;
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
                    printf("connect to %s:%d\n",inet_ntoa(cliaddr.sin_addr),  
                            ntohs(cliaddr.sin_port));
                    for(i = 0; i < len; i++)
                    {
                        if(fds[i] == -1)
                        {
                            fds[i] = cli;
                            break;
                        }
                    }
                    if(i == len)
                    {
                        sprintf(outbuf, "Too many connections\n");
                        send(cli,outbuf, strlen(outbuf), 0);
                        close(cli);
                    }
                }
            }
            else        
            {
                for(i = 0; i < len; i++)
                {
                    if(i != 0 && FD_ISSET(fds[i], &rdfs))
                    {
                        break;
                    }
                }
                assert(i != 0 && i < len);
                ret = recv(fds[i],inbuf,SIZE,0);
            	  if(ret == 0)
            	  {   
            	      close(fds[i]);
            	      fds[i] = -1;
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
                    close(fds[i]);
                    fds[i] = -1;
                    continue;
                }
                
                if(strncmp(inbuf, "addnode", 7) == 0)
                {
                    char* ip  = inbuf + 7;
                    char* port;
                    
                    while(*ip == ' ') ip++;
                    port = ip;
                    while(*port != ':') port++;
                    *port = '\0';
                    port ++;
                    
                    for(j = 0; j < MAX_NODE; j++)
                    {
                        if(pnode[j] == NULL)
                        {
                            pnode[j] = (struct mood_dh_node*)malloc(sizeof(struct mood_dh_node));
                            break;
                        }
                    }
                    
                    if(j < MAX_NODE)
                    {
                        strcpy(pnode[j]->ip, ip);
                        pnode[j]->port = atoi(port);
                        pnode[j]->socket = socket(AF_INET,SOCK_STREAM,0);

					              nodeaddr.sin_family =AF_INET;  
	                      nodeaddr.sin_port   = htons(pnode[j]->port);
	                      nodeaddr.sin_addr.s_addr=inet_addr(ip);
	                      printf("\nhostname = %s, port = %s\n", ip, port);
	                      ret = connect(pnode[j]->socket,(struct sockaddr*)&nodeaddr,sizeof(nodeaddr));
	                      if(ret < 0)
	                      {
	                          free(pnode[j]);
	                          pnode[j] = NULL;
                            sprintf(outbuf, "Failed to add node, %s:%s", ip, port);
            	              send(fds[i],outbuf, strlen(outbuf), 0);
	                      }
	                      else
	                      {
                            sprintf(outbuf, "New node added, %s:%s", ip, port);
                            printf("%s", outbuf);
            	              send(fds[i],outbuf, strlen(outbuf), 0);
	                      }
                    }
                    continue;
                }
                sprintf(outbuf, "Hello: %s", inbuf);
                send(fds[i],outbuf, strlen(outbuf), 0);
                for(j = 0; j < MAX_NODE; j++)
                {
                    if(pnode[j])
                    {
                        send(pnode[j]->socket,outbuf, strlen(outbuf), 0);
                    }
                }
            }
        }
    }
    return 1;  
}