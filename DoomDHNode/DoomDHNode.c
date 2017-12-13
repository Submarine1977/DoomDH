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
#include "../Command.h"

#define DEBUG            0
#define BUFFER_SIZE      65536
#define MAX_NODE_COUNT   1024
#define MAX_SERVER_COUNT 128
#define MAX_DB_COUNT     128
                              
int      nodeid;                                   
char     outbuf[BUFFER_SIZE];  
char     debuglogfile[128];

int commandno = 1;

struct doom_dh_node
{
    int   socket;
    char  ip[16];
    int   port;   
    
    struct doom_dh_command_node *firstcommand,*lastcommand;
    char  buffer[BUFFER_SIZE*2];
    int   bufferlength;
};
struct    doom_dh_node*  psiblingnodes[MAX_NODE_COUNT];

struct doom_dh_server
{
    int   socket;
    char  ip[16];
    int   port;
    
    struct doom_dh_command_node *firstcommand,*lastcommand;
    char  buffer[BUFFER_SIZE*2];
    int   bufferlength;
};
struct    doom_dh_server*  pservers[MAX_SERVER_COUNT];

void dumpbuffer(FILE *f, char *buffer, int length)
{
    int i;
    char str[16];
    for(i = 0; i < length; i++)
    {
        sprintf(str, "%02x", buffer[i]);
        fprintf(f, "%s ", str + strlen(str) - 2);
        if((i + 1) % 32 == 0)
        {
            printf("\n");
        }
    }
    printf("\n");
};


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

void add_siblingcommand(struct doom_dh_node* pnode, struct doom_dh_command_node *pcommand)
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

void add_command(struct doom_dh_server* pserver, struct doom_dh_command_node *pcommand)
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

struct doom_dh_command_node *get_command(int commandno, struct doom_dh_server **ppser)
{
    int i;
    struct doom_dh_command_node* pcommand = NULL;
    for(i = 0; i < MAX_SERVER_COUNT; i++)
    {
        pcommand = pservers[i]->firstcommand;
        while(pcommand != NULL)
        {
            if(pcommand->command.no == commandno)
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

int execute_ddl(char* buffer, int server)
{
    int rc;
    char *errmsg;
    if(pcurrentdb == NULL)
    {
        sprintf(outbuf + 4, "error:please set current database first!\n");
        outbuf[0]              = COMMAND_EXECUTEDDL;
        outbuf[1]              = COMMAND_ACTION_RETOUT_CLIENT;
        *(short*)(outbuf + 2) = strlen(outbuf + 4) + 4 + 1; // + 1 for '\0'
        send(server,outbuf, *(short*)(outbuf + 2), 0);
        return 0;
    }
    rc = sqlite3_exec(pcurrentdb->db, buffer,NULL,NULL,&errmsg);
    if(rc == SQLITE_OK)
    {
        return 1;
    }
    else
    {
        sprintf(outbuf + 4, "error:%s\n", errmsg);
        outbuf[0]              = COMMAND_EXECUTEDDL;
        outbuf[1]              = COMMAND_ACTION_RETOUT_CLIENT;
        *(short*)(outbuf + 2) = strlen(outbuf + 4) + 4 + 1; // + 1 for '\0'
        send(server,outbuf, *(short*)(outbuf + 2), 0);
        return 1;
    }
};

int execute_dml(char* buffer, int server)
{
    int rc;
    char *errmsg;
    if(pcurrentdb == NULL)
    {
        sprintf(outbuf + 4, "error:please set current database first!\n");
        outbuf[0]              = COMMAND_EXECUTEDML;
        outbuf[1]              = COMMAND_ACTION_RETOUT_CLIENT;
        *(short*)(outbuf + 2) = strlen(outbuf + 4) + 4 + 1; // + 1 for '\0'
        send(server,outbuf, *(short*)(outbuf + 2), 0);
        return 0;
    }
    rc = sqlite3_exec(pcurrentdb->db, buffer,NULL,NULL,&errmsg);
    if(rc == SQLITE_OK)
    {
        return 1;
    }
    else
    {
        sprintf(outbuf + 4, "error:%s\n", errmsg);
        outbuf[0]              = COMMAND_EXECUTEDML;
        outbuf[1]              = COMMAND_ACTION_RETOUT_CLIENT;
        *(short*)(outbuf + 2) = strlen(outbuf + 4) + 4 + 1; // + 1 for '\0'
        send(server,outbuf, *(short*)(outbuf + 2), 0);
        return 1;
    }
};

int init_command(struct doom_dh_command_node *pcommand, char* param, int server)
{
    int i, n, rc;
    char sql[512], str[8], *table;
    table = param;
    if(pcommand->command.id == COMMAND_IMPORTCSV)
    {
        import_csv_count = 0;        
        if(execute_dml("BEGIN;", server)  == 0)
        {
            return 0;
        }
        
        sprintf(sql, "select * from %s;", table);
        rc = sqlite3_prepare (pcurrentdb->db, sql, -1, &import_csv_statement, NULL);
        if(rc != SQLITE_OK)
        {
            sprintf(outbuf + 4, "error:prepare error rc = %d!, sql = %s\n", rc, sql);
            outbuf[0]              = COMMAND_IMPORTCSV;
            outbuf[1]              = COMMAND_ACTION_RETOUT_CLIENT;
            *(short*)(outbuf + 2) = strlen(outbuf + 4) + 4 + 1; // + 1 for '\0'
            send(server,outbuf, *(short*)(outbuf + 2), 0);
            execute_dml("ROLLBACK;", server);
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
            sprintf(outbuf + 4, "error:prepare error rc = %d!, sql = %s", rc, sql);
            outbuf[0]              = COMMAND_IMPORTCSV;
            outbuf[1]              = COMMAND_ACTION_RETOUT_CLIENT;
            *(short*)(outbuf + 2) = strlen(outbuf + 4) + 4 + 1; // + 1 for '\0'
            send(server,outbuf, *(short*)(outbuf + 2), 0);
            execute_dml("ROLLBACK;", server);
            return 0;
        }
    }
    return 1;    
}

int finalize_command(struct doom_dh_command_node *pcommand, int server)
{
    if(pcommand->command.id == COMMAND_IMPORTCSV)
    {
        if(execute_dml("COMMIT;", server)  == 0)
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
    int i, m, n, pos;
    char *errmsg;
    char *field[512];
    n = import_csv_fieldcount;
    pos = 0;
    while( (m = split_string(buffer, ',', field, &pos)) > 0)
    {
        if(m != n)
        {
            sprintf(outbuf + 4, "error: the table has %d field,the line %d of csv has %d field, filed[0] = %s\n", n, import_csv_count, m, field[0]);
            outbuf[0]              = COMMAND_IMPORTCSV;
            outbuf[1]              = COMMAND_ACTION_RETOUT_CLIENT;
            *(short*)(outbuf + 2) = strlen(outbuf + 4) + 4 + 1; // + 1 for '\0'
            send(server,outbuf, *(short*)(outbuf + 2), 0);
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
            execute_dml("COMMIT;", server);
            execute_dml("BEGIN;", server);
        }
    }
}

void execute_dql(char* buffer, int server)
{
    int i, n, rc;
    sqlite3_stmt *statement;

    if(pcurrentdb == NULL)
    {
        sprintf(outbuf + 4, "error:please set current database first!\n");
        outbuf[0]              = COMMAND_EXECUTEDQL;
        outbuf[1]              = COMMAND_ACTION_RETOUT_CLIENT;
        *(short*)(outbuf + 2) = strlen(outbuf + 4) + 4 + 1; // + 1 for '\0'
        send(server,outbuf, *(short*)(outbuf + 2), 0);
        return;
    }
    rc = sqlite3_prepare (pcurrentdb->db, buffer, -1, &statement, NULL);
    if(rc != SQLITE_OK)
    {
        sprintf(outbuf + 4, "error:prepare error rc = %d!", rc);
        outbuf[0]              = COMMAND_EXECUTEDQL;
        outbuf[1]              = COMMAND_ACTION_RETOUT_CLIENT;
        *(short*)(outbuf + 2) = strlen(outbuf + 4) + 4 + 1; // + 1 for '\0'
        send(server,outbuf, *(short*)(outbuf + 2), 0);
    }
    
    n = sqlite3_column_count(statement);
    while(sqlite3_step(statement) == SQLITE_ROW) 
    {
    	  memset(outbuf, 0, BUFFER_SIZE);
        for(i = 0; i < n; i++)
        {
            strcat(outbuf + 4, (char *)sqlite3_column_text(statement, i));
            if(i < n - 1)
            {
                strcat(outbuf, "\t");
            }
            else
            {
                strcat(outbuf, "\n");
            }
        }
        outbuf[0]              = COMMAND_EXECUTEDQL;
        outbuf[1]              = COMMAND_ACTION_RETOUT_CLIENT;
        *(short*)(outbuf + 2) = strlen(outbuf + 4) + 4 + 1; // + 1 for '\0'
        send(server,outbuf, *(short*)(outbuf + 2), 0);
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
            sprintf(outbuf + 4, "Database %s was set as current database!\n", name);
            outbuf[0]              = COMMAND_SETDATABASE;
            outbuf[1]              = COMMAND_ACTION_RETOUT_CLIENT;
            *(short*)(outbuf + 2) = strlen(outbuf + 4) + 4 + 1; // + 1 for '\0'
            send(server,outbuf, *(short*)(outbuf + 2), 0);
            pcurrentdb = &databases[i];
            return &databases[i];
        }
    }
    sprintf(outbuf + 4, "error:could not find the databases %s\n", name);
    outbuf[0]              = COMMAND_SETDATABASE;
    outbuf[1]              = COMMAND_ACTION_RETOUT_CLIENT;
    *(short*)(outbuf + 2) = strlen(outbuf + 4) + 4 + 1; // + 1 for '\0'
    send(server,outbuf, *(short*)(outbuf + 2), 0);
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
        char dbname[128];
        sprintf(dbname, "/data/users/guozhaozhong/DoomDH/%d_%s.sq3", nodeid, name);
        rc = sqlite3_open(dbname, &databases[i].db);
        //rc = sqlite3_open(":memory:", &databases[i].db);
        if(rc == SQLITE_OK)
        {
            sprintf(outbuf + 4, "Database %s was created!\n", name);
            outbuf[0]              = COMMAND_CREATEDATABASE;
            outbuf[1]              = COMMAND_ACTION_RETOUT_CLIENT;
            *(short*)(outbuf + 2) = strlen(outbuf + 4) + 4 + 1; // + 1 for '\0'
            send(server,outbuf, *(short*)(outbuf + 2), 0);
            return &databases[i];
        }
        else
        {
            sprintf(outbuf + 4, "error:Failed to open database %s\n", sqlite3_errmsg(databases[i].db));
            outbuf[0]              = COMMAND_CREATEDATABASE;
            outbuf[1]              = COMMAND_ACTION_RETOUT_CLIENT;
            *(short*)(outbuf + 2) = strlen(outbuf + 4) + 4 + 1; // + 1 for '\0'
            send(server,outbuf, *(short*)(outbuf + 2), 0);
            databases[i].db = NULL;
            databases[i].name[0] = '\0';
            return NULL;
        }
    }
    else
    {
        sprintf(outbuf + 4, "error:Failed to create database, too many databases\n");
        outbuf[0]              = COMMAND_CREATEDATABASE;
        outbuf[1]              = COMMAND_ACTION_RETOUT_CLIENT;
        *(short*)(outbuf + 2) = strlen(outbuf + 4) + 4 + 1; // + 1 for '\0'
        send(server,outbuf, *(short*)(outbuf + 2), 0);
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
        sprintf(outbuf + 4, "ID:%d\n", nodeid);
        outbuf[0]              = COMMAND_GETNODEINFO;
        outbuf[1]              = COMMAND_ACTION_RETOUT_SERVER;
        *(short*)(outbuf + 2) = strlen(outbuf + 4) + 4 + 1; // + 1 for '\0'
        send(server,outbuf, *(short*)(outbuf + 2), 0);
        return;
    }
    sprintf(outbuf + 4, "error:do not have the %s infomation\n", name);
    outbuf[0]              = COMMAND_GETNODEINFO;
    outbuf[1]              = COMMAND_ACTION_RETOUT_SERVER;
    *(short*)(outbuf + 2) = strlen(outbuf + 4) + 4 + 1; // + 1 for '\0'
    send(server,outbuf, *(short*)(outbuf + 2), 0);
}
struct doom_dh_node* add_sibling_node(char* buffer, int server)
{
    int j, ret;
    char* ip;
    char* port;
    struct sockaddr_in nodeaddr;  
                    
    ip = buffer;

    port = ip;
    while(*port != ':')
    {
        if(*port == '\0')
        {
            sprintf(outbuf + 4, "error:Wrong sibling node format%s\n", buffer);
            outbuf[0]              = COMMAND_ADDSIBLINGNODE;
            outbuf[1]              = COMMAND_ACTION_RETOUT_CLIENT;
            *(short*)(outbuf + 2) = strlen(outbuf + 4) + 4 + 1; // + 1 for '\0'
            send(server,outbuf, *(short*)(outbuf + 2), 0);
            return NULL;
        }	
        port++;
    }
    *port = '\0';
    port ++;

    if(DEBUG)
    {
        FILE *f = fopen(debuglogfile, "a+");
        fprintf(f,"add_sibling_node %s %s\n", ip, port);
        fclose(f);
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
        sprintf(outbuf + 4, "error:Failed to add sibling node %s %s, node was already connected, or port was already used\n", ip, port);
        outbuf[0]              = COMMAND_ADDSIBLINGNODE;
        outbuf[1]              = COMMAND_ACTION_RETOUT_CLIENT;
        *(short*)(outbuf + 2) = strlen(outbuf + 4) + 4 + 1; // + 1 for '\0'
        send(server,outbuf, *(short*)(outbuf + 2), 0);
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
        psiblingnodes[j]->bufferlength  = 0;

        nodeaddr.sin_family =AF_INET;  
        nodeaddr.sin_port   = htons(psiblingnodes[j]->port);
        nodeaddr.sin_addr.s_addr=inet_addr(ip);
        ret = connect(psiblingnodes[j]->socket,(struct sockaddr*)&nodeaddr,sizeof(nodeaddr));
        if(ret < 0)
        {
            free(psiblingnodes[j]);
            psiblingnodes[j] = NULL;
            sprintf(outbuf + 4, "error: Failed to add sibling node, %s:%s, errno=%d\n", ip, port, errno);
            outbuf[0]              = COMMAND_ADDSIBLINGNODE;
            outbuf[1]              = COMMAND_ACTION_RETOUT_CLIENT;
            *(short*)(outbuf + 2) = strlen(outbuf + 4) + 4 + 1; // + 1 for '\0'
            send(server,outbuf, *(short*)(outbuf + 2), 0);
            return NULL;
        }
        else
        {
            sprintf(outbuf + 4, "info:New sibling node added, %s:%s\n", ip, port);
            outbuf[0]              = COMMAND_ADDSIBLINGNODE;
            outbuf[1]              = COMMAND_ACTION_RETOUT_CLIENT;
            *(short*)(outbuf + 2) = strlen(outbuf + 4) + 4 + 1; // + 1 for '\0'
            send(server,outbuf, *(short*)(outbuf + 2), 0);
            return psiblingnodes[j];
        }
    }
    else
    {
        sprintf(outbuf + 4, "error:Failed to add sibling node, too many nodes\n");
        outbuf[0]              = COMMAND_ADDSIBLINGNODE;
        outbuf[1]              = COMMAND_ACTION_RETOUT_CLIENT;
        *(short*)(outbuf + 2) = strlen(outbuf + 4) + 4 + 1; // + 1 for '\0'
        send(server,outbuf, *(short*)(outbuf + 2), 0);
        return NULL;
    }
};

//handle server request
void handle_server_request(int index)
{
		int  i, j, ret;
    char *start;
    struct doom_dh_command_node *pcommand, *pchildcommand;    
    
    ret = recv(pservers[index]->socket, pservers[index]->buffer + pservers[index]->bufferlength, BUFFER_SIZE - pservers[index]->bufferlength, 0);
    if(ret == 0)
    {   
        close(pservers[index]->socket);
        printf("Server connection %s:%d closed!\n", pservers[index]->ip, pservers[index]->port);
        free(pservers[index]);
        pservers[index] = NULL;
        return;
    }
    else if(ret < 0)
    {
        printf("error recieve data, errno = %d\n", errno);
        return;
    }
    //ret > 0
    pservers[index]->bufferlength += ret;
    if(DEBUG)
    {
        FILE *f = fopen(debuglogfile, "a+");
        fprintf(f, "handle_client_request[%d] buffer = \n", index);
        dumpbuffer(f, pservers[index]->buffer, pservers[index]->bufferlength);
        fclose(f);
    }

    if( pservers[index]->buffer[0] == COMMAND_QUIT)
    {
        close(pservers[index]->socket);
        printf("Connection %s:%d closed!\n", pservers[index]->ip, pservers[index]->port);
        free(pservers[index]);
        pservers[index] = NULL;
    }

    i = 0;
    while(1)
    {
		    start  = pservers[index]->buffer + i;
		    if( pservers[index]->bufferlength < i + 4 ||
		    	  pservers[index]->bufferlength < i + *(short*)(start + 2))
		    {//command not finished
		        break;
		    }

        if(DEBUG)
        {
            FILE *f = fopen(debuglogfile, "a+");
            fprintf(f, "handle_server_request: server index = %d, start = \n", index);
            dumpbuffer(f, start, *(short*)(start + 2));
            dump_command_status(pservers[index]->firstcommand);
            fclose(f);
        }

        if(pservers[index]->lastcommand == NULL || //no command
        	 pservers[index]->lastcommand->command.waitinginput == 0) //finish input for last command
        {
            if(start[1] == COMMAND_ACTION_EXESTART)
            {
                //create a new command
                pcommand = (struct doom_dh_command_node *)malloc(sizeof(struct doom_dh_command_node));
                pcommand->command.no            = commandno++;
                pcommand->command.parentno      = -1; //this is already a parent command
                pcommand->command.waitinginput  = 0;
                pcommand->command.waitingresult = 0;
                pcommand->prev = pcommand->next = NULL;
                pcommand->command.id            = start[0];
                gettimeofday(&pcommand->command.starttime, NULL);

                if( pcommand->command.id == COMMAND_ADDSIBLINGNODE  || 
                	  pcommand->command.id == COMMAND_GETNODEINFO     ||
                	  pcommand->command.id == COMMAND_CREATEDATABASE  ||
                	  pcommand->command.id == COMMAND_SETDATABASE     ||
                	  pcommand->command.id == COMMAND_EXECUTEDDL      ||
                	  pcommand->command.id == COMMAND_EXECUTEDML      ||
                	  pcommand->command.id == COMMAND_EXECUTEDQL      ||
                	  pcommand->command.id == COMMAND_IMPORTCSV
                	  )
                {
                    pcommand->command.waitinginput = 1;
                    add_command(pservers[index], pcommand);
                    init_command(pcommand, start + 4, pservers[index]->socket);
                    
                    outbuf[0] = pcommand->command.id;
                    outbuf[1] = COMMAND_ACTION_RETSTART;
                    *(short*)(outbuf + 2) = 4;
                    send(pservers[index]->socket,outbuf, 4, 0);
                }
                else
                {
                    printf("error MNG command %d\n", pcommand->command.id);
                    free(pcommand);
                }
            }
            else
            {
                printf("the server is waiting for starting a command, but the input command action is %d\n", start[1]);
            }
        }
        else if(pservers[index]->lastcommand != NULL && pservers[index]->lastcommand->command.waitinginput == 1) //waiting for input
        {
            if(start[1] == COMMAND_ACTION_EXEINPUT || start[1] == COMMAND_ACTION_EXEINPUT_ONE)
            {	
                int offset;
                if(start[1] == COMMAND_ACTION_EXEINPUT_ONE)
                {
                    offset = 6;
                }
                else
                {
                    offset = 4;
                }
                if(pservers[index]->lastcommand->command.id == COMMAND_ADDSIBLINGNODE)	
                {
                    add_sibling_node(start + offset, pservers[index]->socket);
                }
                else if(pservers[index]->lastcommand->command.id == COMMAND_GETNODEINFO)	
                {
                    get_node_info(start + offset, pservers[index]->socket);
                }
                else if(pservers[index]->lastcommand->command.id == COMMAND_CREATEDATABASE)	
                {
                    create_database(start + offset, pservers[index]->socket);
                }
                else if(pservers[index]->lastcommand->command.id == COMMAND_SETDATABASE)	
                {
                    set_database(start + offset, pservers[index]->socket);
                }
                else if(pservers[index]->lastcommand->command.id == COMMAND_EXECUTEDDL)	
                {
                    execute_ddl(start + offset, pservers[index]->socket);
                }
                else if(pservers[index]->lastcommand->command.id == COMMAND_EXECUTEDML)	
                {
                    execute_dml(start + offset, pservers[index]->socket);
                }
                else if(pservers[index]->lastcommand->command.id == COMMAND_EXECUTEDQL)	
                {
                    execute_dql(start + offset, pservers[index]->socket);
                }
                else if(pservers[index]->lastcommand->command.id == COMMAND_IMPORTCSV)	//import csv into [table]
                {
                    import_csv(start + offset, pservers[index]->socket);
                }
                else
                {
                    printf("error request command=%d\n", pservers[index]->lastcommand->command.id);
                }
            }
            else
            {
                finalize_command(pservers[index]->lastcommand, pservers[index]->socket);
                pservers[index]->lastcommand->command.waitinginput = 0; //no need to wait for input

                struct timeval endtime;
                gettimeofday(&endtime, NULL);
                long int sec, usec;
                sec  = endtime.tv_sec - pservers[index]->lastcommand->command.starttime.tv_sec;
                usec = endtime.tv_usec - pservers[index]->lastcommand->command.starttime.tv_usec;
                if(usec < 0)
                {
                    sec --;
                    usec = 1000000 + usec;
                }
                sprintf(outbuf + 4, "%ldm%ldus\n", sec, usec);
                outbuf[0] = pservers[index]->lastcommand->command.id;
                outbuf[1] = COMMAND_ACTION_RETSTOP;
                *(short*)(outbuf + 2) = strlen(outbuf + 4) + 4 + 1; //+1 for "\0"
                send(pservers[index]->socket,outbuf, *(short*)(outbuf + 2), 0);
                
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
        i += *(short*)(start+2);
    }
    if(i < pservers[index]->bufferlength)
    {
        memmove(pservers[index]->buffer, pservers[index]->buffer + i, pservers[index]->bufferlength - i);
    }
    pservers[index]->bufferlength -= i;

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
    
    sprintf(debuglogfile, "debug_%d.log", nodeid);

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
                            pservers[i]->bufferlength  = 0;
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
                            psiblingnodes[i]->bufferlength = 0;
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