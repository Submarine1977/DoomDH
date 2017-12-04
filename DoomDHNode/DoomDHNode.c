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

int    fds[2];
const int len = 2;

int main(int argc, char* argv[])  
{
    int    i, ret;
    int    ser,cli;  
    char   inbuf[SIZE];
    char   outbuf[SIZE];  
    struct sockaddr_in seraddr,cliaddr;  
    fd_set  rdfs;
    int     port;
    
    if(argc != 2)
    {
        printf("Usage: %s PORT_NUMBER\n", argv[0]);
        return 1;
    }
    port = atoi(argv[1]);
    
    socklen_t clilen        = sizeof(cliaddr);  
 
    seraddr.sin_family      = AF_INET;  
    seraddr.sin_addr.s_addr = htonl(INADDR_ANY);  
    seraddr.sin_port        = htons(port);  
    ser    = socket(AF_INET,SOCK_STREAM,0);  
    ret    = bind(ser,(struct sockaddr*)&seraddr,sizeof(seraddr));  

    ret    = listen(ser,5); 
    if(ret < 0)
    {
        printf("Failed to listen on port %d, errno = %d\n", port, errno);
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
                if(strcmp(inbuf, "quit\r\n") == 0)
                {
                    close(fds[i]);
                    fds[i] = -1;
                    continue;
                }
                printf("%s\n", inbuf);
                //sprintf(outbuf, "Hello: %s", inbuf);
                //send(fds[i],outbuf, strlen(outbuf), 0);
            }
        }
    }
    return 1;  
}