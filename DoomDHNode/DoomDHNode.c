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

#define DEBUG            0
#define BUFFER_SIZE      1024
#define MAX_NODE_COUNT   1024
#define MAX_SERVER_COUNT 128
#define MAX_DB_COUNT     128
                              
int       nodeid;                                   
char      inbuf[BUFFER_SIZE];
char      outbuf[BUFFER_SIZE];  

int commandid = 1;
struct doom_dh_command
{
    int     id;
    int     parentid;
    char    name[32];
    int     waitinginput;
    int     waitingresult;
    struct  timeval starttime;
};

struct doom_dh_command_list
{
    struct doom_dh_command command;
    struct doom_dh_command_list *prev, *next;
};

struct doom_dh_node
{
    int   socket;
    char  ip[16];
    int   port;   
    
    struct doom_dh_command_list *firstcommand,*lastcommand;
    char  buffer[BUFFER_SIZE*2];
};
struct    doom_dh_node*  psiblingnodes[MAX_NODE_COUNT];

struct doom_dh_server
{
    int   socket;
    char  ip[16];
    int   port;
    
    struct doom_dh_command_list *firstcommand,*lastcommand;
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

sqlite3_stmt *import_csv_statement;
int          import_csv_fieldcount;
int          import_csv_count;

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

int split_string(char* buffer, char sperator, char** field)
{
    int m = 0;
    int i = 0, j = 0;
    
    field[m] = buffer;
    m++;
    while(buffer[i] != '\0')
    {
        if(buffer[i] == sperator)
        {
            buffer[i] = '\0';
            field[m] = buffer + i + 1;
            m++;
        }
        i++;
    }
    return m;
}

void add_siblingcommand(struct doom_dh_node* pnode, struct doom_dh_command_list *pcommand)
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

void add_command(struct doom_dh_server* pserver, struct doom_dh_command_list *pcommand)
{
    if(pserver->firstcommand == NULL)
    {
        pserver->firstcommand = pserver->lastcommand = pcommand;
    }
    else
    {
        pserver->lastcommand->next = pcommand;
        pcommand->prev = pserver->lastcommand;
        pserver->lastcommand = pcommand;
    }
}

struct doom_dh_command_list *get_command(int commandid, struct doom_dh_server **ppser)
{
    int i;
    struct doom_dh_command_list* pcommand = NULL;
    for(i = 0; i < MAX_SERVER_COUNT; i++)
    {
        pcommand = pservers[i]->firstcommand;
        while(pcommand != NULL)
        {
            if(pcommand->command.id == commandid)
            {
                *ppser = pservers[i];
                return pcommand;
            }
            pcommand = pcommand->next;
        }
    }
    *ppser = NULL;
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

int execute_ddl_dml(char* buffer, int server)
{
    int rc;
    char *errmsg;
    if(pcurrentdb == NULL)
    {
        sprintf(outbuf, "error:please set current database first!\n");
        send(server,outbuf, strlen(outbuf), 0);
        return 0;
    }
    rc = sqlite3_exec(pcurrentdb->db, buffer,NULL,NULL,&errmsg);
    if(rc == SQLITE_OK)
    {
        return 1;
    }
    else
    {
        sprintf(outbuf, "error:%s\n", errmsg);
        send(server,outbuf, strlen(outbuf), 0);
        return 1;
    }
};

int init_command(struct doom_dh_command_list *pcommand, int server)
{
    int i, n, rc;
    char sql[512], str[8], *table;
    table = pcommand->command.name + strlen("import csv into");
    if(strncasecmp(pcommand->command.name, "import csv into", strlen("import csv into")) == 0)
    {
        import_csv_count = 0;        
        if(execute_ddl_dml("BEGIN;", server)  == 0)
        {
            return 0;
        }
        
        sprintf(sql, "select * from %s;", table);
        rc = sqlite3_prepare (pcurrentdb->db, sql, -1, &import_csv_statement, NULL);
        if(rc != SQLITE_OK)
        {
            sprintf(outbuf, "error:prepare error rc = %d!, sql = %s", rc, sql);
            send(server,outbuf, strlen(outbuf), 0);
            execute_ddl_dml("ROLLBACK;", server);
            return 0;
        }
        n = sqlite3_column_count(import_csv_statement);
        sqlite3_finalize(import_csv_statement);
        import_csv_fieldcount = n;
        
        //insert into [table] values (?1,?2,?3,..?n);
        sprintf(sql, "insert into %s values (", table);
        for(i = 0; i < n; i++)
        {
            if(i < n - 1)
            {
                sprintf(str, "?%d,", i + 1);
            }
            else
            {
                sprintf(str, "?%d);", i + 1);
            }
            strcat(sql, str);
        }
        rc = sqlite3_prepare (pcurrentdb->db, sql, -1, &import_csv_statement, NULL);
        if(rc != SQLITE_OK)
        {
            sprintf(outbuf, "error:prepare error rc = %d!, sql = %s", rc, sql);
            send(server,outbuf, strlen(outbuf), 0);
            execute_ddl_dml("ROLLBACK;", server);
            return 0;
        }
    }
    return 1;    
}

int finalize_command(struct doom_dh_command_list *pcommand, int server)
{
    if(strncasecmp(pcommand->command.name, "import csv into", strlen("import csv into")) == 0)
    {
        if(execute_ddl_dml("COMMIT;", server)  == 0)
        {
            return 1;
        }
        sqlite3_finalize(import_csv_statement);
        import_csv_statement = NULL;
    }
    return 0;
}


void import_csv(char* buffer, int server)
{
    int i, m, n;
    char *errmsg;
    char *field[512];
    
    
    n = import_csv_fieldcount;
    m = split_string(buffer, ',', field);
    
    if(m != n)
    {
        sprintf(outbuf, "error: the table has %d field,the line %d of csv has %d field\n", n, import_csv_count, m);
        send(server,outbuf, strlen(outbuf), 0);
    }
    else
    {
        for(i = 0; i < n; i++)
        {
            sqlite3_bind_text(import_csv_statement, i + 1, field[i], -1, SQLITE_STATIC);
        }
        sqlite3_step(import_csv_statement);
        sqlite3_reset(import_csv_statement);
    }
    import_csv_count++;
    if(import_csv_count % 1000 == 0)
    {
        execute_ddl_dml("COMMIT;", server);
        execute_ddl_dml("BEGIN;", server);
    }
}

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
    
    n = sqlite3_column_count(statement);
    while(sqlite3_step(statement) == SQLITE_ROW) 
    {
    	  memset(outbuf, 0, BUFFER_SIZE);
        for(i = 0; i < n; i++)
        {
            strcat(outbuf, (char *)sqlite3_column_text(statement, i));
            if(i < n - 1)
            {
                strcat(outbuf, "\t");
            }
            else
            {
                strcat(outbuf, "\n");
            }
        }
        send(server,outbuf, strlen(outbuf), 0);
    }
    sqlite3_finalize(statement);
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
        //char dbname[128];
        //sprintf(dbname, "/data/users/guozhaozhong/DoomDH/%d_%s.sq3", nodeid, name);
        //rc = sqlite3_open(dbname, &databases[i].db);
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
        sprintf(outbuf, "ID:%d\n", nodeid);
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
                    
    ip = skip_space(buffer);

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

    if(DEBUG)
    {
        printf("add_sibling_node %s %s\n", ip, port);
    }     
    
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
        psiblingnodes[j]->firstcommand  = NULL;
        psiblingnodes[j]->lastcommand   = NULL;

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
void handle_server_request(int index)
{
		int  j, t, ret;
    char *str, *start, *end;
    struct doom_dh_command_list *pcommand, *pchildcommand;    
    
    ret = recv(pservers[index]->socket,inbuf,BUFFER_SIZE,0);
    if(ret == 0)
    {   
        close(pservers[index]->socket);
        printf("Server connection %s:%d closed!\n", pservers[index]->ip, pservers[index]->port);
        free(pservers[index]);
        pservers[index] = NULL;
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

    if( pservers[index]->firstcommand == 0 && //not waiting for input/result
	      strncasecmp(inbuf, "quit", strlen("quit")) == 0 )
    {
        close(pservers[index]->socket);
        printf("Connection %s:%d closed!\n", pservers[index]->ip, pservers[index]->port);
        free(pservers[index]);
        pservers[index] = NULL;
    }
    strcat(pservers[index]->buffer, inbuf);

    while(1)
    {
        //get one line
        str   = skip_space(pservers[index]->buffer);
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
            printf("handle_server_request: start = %s, server index = %d\n", start, index);
            dump_command_status(pservers[index]->firstcommand);
        }

        if(pservers[index]->lastcommand == NULL || //no command
        	 pservers[index]->lastcommand->command.waitinginput == 0) //finish input for last command
        {
            if(DEBUG)
            {
                printf("handle_server_request handle input start = %s\n", start);
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
                gettimeofday(&pcommand->command.starttime, NULL);

                if( strcasecmp(pcommand->command.name, "add sibling node") != 0 && 
                	  strcasecmp(pcommand->command.name, "get node info") != 0)
                {
                    printf("error MNG command %s\n", pcommand->command.name);
                    free(pcommand);
                }
                else
                {
                    pcommand->command.waitinginput = 1;
                    add_command(pservers[index], pcommand);
                    
                    sprintf(outbuf, "RES:%s\n", pcommand->command.name);
                    send(pservers[index]->socket,outbuf, strlen(outbuf), 0);
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
                gettimeofday(&pcommand->command.starttime, NULL);
                
                if( strcasecmp(pcommand->command.name, "create database")  != 0 &&
                	  strcasecmp(pcommand->command.name, "set database")     != 0 &&
                	  strcasecmp(pcommand->command.name, "execute ddl")      != 0 &&
                	  strcasecmp(pcommand->command.name, "execute dml")      != 0 &&
                	  strcasecmp(pcommand->command.name, "execute dql")      != 0 &&
                	  strncasecmp(pcommand->command.name, "import csv into", strlen("import csv into"))  != 0 ) //import csv into [table]
                {
                    printf("error TSK command %s\n", pcommand->command.name);
                    free(pcommand);
                }
                else
                {
                    pcommand->command.waitinginput = 1;
                    add_command(pservers[index], pcommand);
                    init_command(pcommand, pservers[index]->socket);
                    
                    sprintf(outbuf, "RES:%s\n", pcommand->command.name);
                    send(pservers[index]->socket,outbuf, strlen(outbuf), 0);
                }
            }
            else
            {
                printf("error request %s\n", start);
            }
        }
        else if(pservers[index]->lastcommand != NULL && pservers[index]->lastcommand->command.waitinginput == 1) //waiting for input
        {
            if(strcasecmp(start, "/MNG") != 0 && strcasecmp(start, "/TSK") != 0 )
            {	
                if(strcasecmp(pservers[index]->lastcommand->command.name, "add sibling node") == 0)	
                {
                    add_sibling_node(start, pservers[index]->socket);
                }
                else if(strcasecmp(pservers[index]->lastcommand->command.name, "get node info") == 0)	
                {
                    get_node_info(start, pservers[index]->socket);
                }
                else if(strcasecmp(pservers[index]->lastcommand->command.name, "create database") == 0)	
                {
                    create_database(start, pservers[index]->socket);
                }
                else if(strcasecmp(pservers[index]->lastcommand->command.name, "set database") == 0)	
                {
                    set_database(start, pservers[index]->socket);
                }
                else if(strcasecmp(pservers[index]->lastcommand->command.name, "execute ddl") == 0)	
                {
                    execute_ddl_dml(start, pservers[index]->socket);
                }
                else if(strcasecmp(pservers[index]->lastcommand->command.name, "execute dml") == 0)	
                {
                    execute_ddl_dml(start, pservers[index]->socket);
                }
                else if(strcasecmp(pservers[index]->lastcommand->command.name, "execute dql") == 0)	
                {
                    execute_dql(start, pservers[index]->socket);
                }
                else if(strncasecmp(pservers[index]->lastcommand->command.name, "import csv into", strlen("import csv into")) == 0)	//import csv into [table]
                {
                    import_csv(start, pservers[index]->socket);
                }
                else
                {
                    printf("error request %s, command=%s\n", start, pservers[index]->lastcommand->command.name);
                }
            }
            else
            {
                finalize_command(pservers[index]->lastcommand, pservers[index]->socket);
                pservers[index]->lastcommand->command.waitinginput = 0; //no need to wait for input

                struct timeval endtime;
                gettimeofday(&endtime, NULL);
                sprintf(outbuf, "/RES %s, %ldm%ldus\n", pservers[index]->lastcommand->command.name, 
                                 endtime.tv_sec - pservers[index]->lastcommand->command.starttime.tv_sec,
                                 endtime.tv_usec - pservers[index]->lastcommand->command.starttime.tv_usec);
                send(pservers[index]->socket,outbuf, strlen(outbuf), 0);
                
                if(pservers[index]->lastcommand->command.waitingresult == 0)
                {
                    //remove last command;
                    pservers[index]->lastcommand = pservers[index]->lastcommand->prev;
                    if(pservers[index]->lastcommand == NULL)
                    {
                        free(pservers[index]->firstcommand);
                        pservers[index]->firstcommand = NULL;
                    }
                    else
                    {
                        free(pservers[index]->lastcommand->next);
                        pservers[index]->lastcommand->next = NULL;
                    }
                }
            }
        }
        else
        {
            printf("node is not ready to handle request\n");
            dump_command_status(pservers[index]->firstcommand);
        }
        j = 0;
        while(str[j] != '\0')
        {
            pservers[index]->buffer[j] = str[j];
            j++;
        }
        pservers[index]->buffer[j] = '\0';
    }
};

//handle node request
void handle_node_request(int index)
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
    
    nodeid   = atoi(argv[1]);
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
                    printf("pservers[%d](%s:%d)\n", i, pservers[i]->ip, pservers[i]->port);
                    dump_command_status(pservers[i]->firstcommand);
                }
            }
            printf("\nsibling node socket status:\n");
            for(j = 0; j < MAX_NODE_COUNT; j++)
            {
                if(psiblingnodes[j] != NULL)
                {
                    printf("psiblingnodes[%d](%s:%d)\n", j, psiblingnodes[j]->ip, psiblingnodes[j]->port);
                    dump_command_status(psiblingnodes[j]->firstcommand);
                }
            }
        }
        //dump end
        
        for(i = 0; i < MAX_SERVER_COUNT; i++)
        {
            if(pservers[i] != NULL)
            {
                FD_SET(pservers[i]->socket, &rdfs);
                max_fd = pservers[i]->socket > max_fd ? pservers[i]->socket : max_fd;
            }
        }
        for(i = 0; i < MAX_NODE_COUNT; i++)
        {
            if(psiblingnodes[i] != NULL && psiblingnodes[i]->firstcommand != NULL) //only receive the msg from nodes who are waiting for result. 
            {
                FD_SET(psiblingnodes[i]->socket, &rdfs);
                max_fd = psiblingnodes[i]->socket > max_fd ? psiblingnodes[i]->socket : max_fd;
                if(DEBUG)
                {
                    printf("select from psiblingnodes[%d]\n", i);
                }
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
                            pservers[i]->firstcommand  = NULL;
                            pservers[i]->lastcommand   = NULL;
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
                            psiblingnodes[i]->firstcommand = NULL;
                            psiblingnodes[i]->lastcommand  = NULL;
                            psiblingnodes[i]->buffer[0]    = '\0';
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
                        handle_server_request(i);	
                    }
                }
                for(j = 0; j < MAX_NODE_COUNT; j++)
                {
                    if(psiblingnodes[j] != NULL && FD_ISSET(psiblingnodes[j]->socket, &rdfs))
                    {
                        handle_node_request(j);
                    }
                }
            }
        }
    }
    return 1;  
}