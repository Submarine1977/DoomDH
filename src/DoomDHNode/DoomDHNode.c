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
#include <time.h>
#include <sys/time.h>

#include "../Sqlite/sqlite3.h"
#include "../Command.h"

#define DEBUG            0
#define BUFFER_SIZE      65536
#define MAX_NODE_COUNT   1024
#define MAX_SERVER_COUNT 128
#define MAX_DB_COUNT     128
                              
int      nodeid;                                   
char     outbuf[BUFFER_SIZE];  

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

int log_info(char *fmt, ... )
{
    time_t timep; 
    int     n;
    va_list args;
    char   filename[64];
    FILE *f;
    char strtime[128], *p;
    
    sprintf(filename, "./log_%d.txt", nodeid);
    f = fopen(filename, "a+");

    time (&timep); 
    sprintf(strtime, "%s", ctime(&timep));
    p = strtime + strlen(strtime) - 1;
    while(*p == '\n' || *p == '\r')
    {
        *p = '\0';
        p--;
    }
    fprintf(f, "[%s]",strtime);

    va_start(args, fmt);
    n = vfprintf(f, fmt, args);
    va_end(args);

    fclose(f);
    return n;    
}

void dumpbuffer(char *buffer, int length)
{
    int i;
    char   filename[64];
    FILE *f;
    char str[16];

    sprintf(filename, "./log_%d.txt", nodeid);
    f = fopen(filename, "a+");
    for(i = 0; i < length; i++)
    {
        sprintf(str, "%02x", buffer[i]);
        fprintf(f, "%s ", str + strlen(str) - 2);
        if((i + 1) % 32 == 0)
        {
            fprintf(f, "\n");
        }
    }
    fprintf(f, "\n");
    fclose(f);
}

void dump_command_status(struct doom_dh_command_node *pcommand)
{
    char   filename[64];
    FILE *f;
    sprintf(filename, "./log_%d.txt", nodeid);
    f = fopen(filename, "a+");
    while(pcommand != NULL)
    {
        fprintf(f, "Command %d: no=%d,parentno=%d, waitinginput=%d, waitingresult=%d\n", 
                   pcommand->command.id, pcommand->command.no, pcommand->command.parentno, pcommand->command.waitinginput, pcommand->command.waitingresult);
        pcommand = pcommand->next;
    }
    fclose(f);
};

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
        *pos = i + 1;
    }
    else //buffer[i] = '\0'
    {
        *pos = i;
    }
    
    return m;
}

int decodehexchar(char c)
{
    if(c >= '0' && c <= '9')
    {
        return c - '0';
    }
    else if(c >= 'a' && c <= 'f')
    {
        return c - 'a' + 10;
    }
    else if(c >= 'A' && c <= 'F')
    {
        return c - 'A' + 10;
    }
    assert(0);
    return 0;
}
int decodehexstring(char * buffer)
{
    int i;
    i = 0;
    while(buffer[2*i] != 0 && buffer[2*i + 1] != 0)
    {
        buffer[i] = (decodehexchar(buffer[2*i]) << 4) + decodehexchar(buffer[2*i + 1]);
        i++;
    }
    buffer[i] = '\0';
    return i;
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
        send_info( server, COMMAND_EXECUTEDDL, COMMAND_ACTION_RETOUT_CLIENT, "error:please set current database first!");
        return 0;
    }
    rc = sqlite3_exec(pcurrentdb->db, buffer,NULL,NULL,&errmsg);
    if(rc == SQLITE_OK)
    {
        return 1;
    }
    else
    {
        send_info( server, COMMAND_EXECUTEDDL, COMMAND_ACTION_RETOUT_CLIENT, "error:%s", errmsg);
        return 1;
    }
};

int execute_dml(char* buffer, int server)
{
    int rc;
    char *errmsg;
    if(pcurrentdb == NULL)
    {
        send_info( server, COMMAND_EXECUTEDML, COMMAND_ACTION_RETOUT_CLIENT, "error:please set current database first!");
        return 0;
    }
    rc = sqlite3_exec(pcurrentdb->db, buffer,NULL,NULL,&errmsg);
    if(rc == SQLITE_OK)
    {
        return 1;
    }
    else
    {
        send_info( server, COMMAND_EXECUTEDML, COMMAND_ACTION_RETOUT_CLIENT, "error:%s", errmsg);
        return 1;
    }
};

int init_command(struct doom_dh_command_node *pcommand, char* param, int server)
{
    int i, n, rc;
    char sql[512], str[8], *table;
    table = param;
    if( pcommand->command.id == COMMAND_IMPORTCSV ||
    	  pcommand->command.id == COMMAND_IMPORTCSX)
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
            send_info( server, pcommand->command.id, COMMAND_ACTION_RETOUT_CLIENT, "error:prepare error rc = %d!, sql = %s", rc, sql);
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
            send_info( server, pcommand->command.id, COMMAND_ACTION_RETOUT_CLIENT, "error:prepare error rc = %d!, sql = %s", rc, sql);
            execute_dml("ROLLBACK;", server);
            return 0;
        }
    }
    return 1;    
}

int finalize_command(struct doom_dh_command_node *pcommand, int server)
{
    if(pcommand->command.id == COMMAND_IMPORTCSV ||
    	 pcommand->command.id == COMMAND_IMPORTCSX)
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

//flag 0: normal csv
//flag 1: Hex csv
void import_csv(char* buffer, int server, int flag)
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
            log_info("error: the table has %d field,the line %d of csv has %d field, pos = %d\n", n, import_csv_count, m, pos);
            for(i = 0; i < m; i++)
            {
                log_info("field[%d] = %s\n", i, field[i]);
            }
        }
        else
        {
            for(i = 0; i < n; i++)
            {
                if(flag == 1)
                {
                    decodehexstring(field[i]);
                }
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
        send_info( server, COMMAND_EXECUTEDQL, COMMAND_ACTION_RETOUT_CLIENT, "error:please set current database first!");
        return;
    }
    rc = sqlite3_prepare (pcurrentdb->db, buffer, -1, &statement, NULL);
    if(rc != SQLITE_OK)
    {
        send_info( server, COMMAND_EXECUTEDQL, COMMAND_ACTION_RETOUT_CLIENT, "error:prepare error rc = %d!", rc);
        return;
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
                strcat(outbuf + 4, "\t");
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
            send_info( server, COMMAND_SETDATABASE, COMMAND_ACTION_RETOUT_CLIENT, "Database %s was set as current database!", name);
            pcurrentdb = &databases[i];
            return &databases[i];
        }
    }
    send_info( server, COMMAND_SETDATABASE, COMMAND_ACTION_RETOUT_CLIENT, "error:could not find the databases %s", name);
    return NULL;
}

void create_ddhtables(sqlite3 *db, int server)
{
    char buf[512], sqlfile[512], sql[8192],*errmsg;
    char tables[][8] = {"DDH_CHA"};
    int  i, rc, count; 
    FILE *f;
    
    count = readlink("/proc/self/exe", buf, 512);
    if(count < 0)
    {
        log_info("error: create_ddhtables, failed to get exe path.\n");
        return;
    }
    strcat(buf, "/DDHModel/");
    
    for(i = 0; i < sizeof(tables) /sizeof(char[8]); i++)
    {
        sprintf(sqlfile,"%s%s", buf, tables[i]);
        f = fopen(sqlfile, "r");
        if(f)
        {
            if((rc = fread(sql, 1, sizeof(sql), f)) > 0)
            {
                rc = sqlite3_exec(db, sql,NULL,NULL,&errmsg);
                if(rc != SQLITE_OK)
                {
                    send_info( server, COMMAND_EXECUTEDDL, COMMAND_ACTION_RETOUT_CLIENT, "error:%s", errmsg);
                }
            }
            else
            {
                log_info("error: create_ddhtables, failed to read file %s, rc = %d\n", sqlfile, rc);
            }
        }
        else
        {
            log_info("error: create_ddhtables, failed to open file %s\n", sqlfile);
        }
    }
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
            send_info( server, COMMAND_CREATEDATABASE, COMMAND_ACTION_RETOUT_CLIENT, "Database %s was created!", name);
            
            //create DDH tables
            create_ddhtables(databases[i].db, server);
            
            return &databases[i];
        }
        else
        {
            send_info( server, COMMAND_CREATEDATABASE, COMMAND_ACTION_RETOUT_CLIENT, "error:Failed to open database %s", sqlite3_errmsg(databases[i].db));
            databases[i].db = NULL;
            databases[i].name[0] = '\0';
            return NULL;
        }
    }
    else
    {
        send_info( server, COMMAND_CREATEDATABASE, COMMAND_ACTION_RETOUT_CLIENT, "error:Failed to create database, too many databases");
        return NULL;
    }
};

void list_database(char* buffer, int server)
{
    int i;
    
    for(i = 0; i < MAX_DB_COUNT; i++)
    {
        if(databases[i].db != NULL)
        {
            if(pcurrentdb != &databases[i])
            {
                send_info( server, COMMAND_LISTDATABASE, COMMAND_ACTION_RETOUT_CLIENT, "%s", databases[i].name);
            }
            else
            {
                send_info( server, COMMAND_LISTDATABASE, COMMAND_ACTION_RETOUT_CLIENT, "%s(*)", databases[i].name);
            }
        }
    }
}

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
        send_info(server, COMMAND_GETNODEINFO, COMMAND_ACTION_RETOUT_CLIENT, "ID:%d", nodeid);
        return;
    }
    send_info( server, COMMAND_GETNODEINFO, COMMAND_ACTION_RETOUT_CLIENT, "error:do not have the %s infomation", name);
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
            send_info( server, COMMAND_ADDSIBLINGNODE, COMMAND_ACTION_RETOUT_CLIENT, "error:Wrong sibling node format%s", buffer);
            return NULL;
        }	
        port++;
    }
    *port = '\0';
    port ++;

    if(DEBUG)
    {
        log_info("add_sibling_node %s %s\n", ip, port);
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
        send_info( server, COMMAND_ADDSIBLINGNODE, COMMAND_ACTION_RETOUT_CLIENT, "error:Failed to add sibling node %s %s, node was already connected, or port was already used", ip, port);
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
            send_info( server, COMMAND_ADDSIBLINGNODE, COMMAND_ACTION_RETOUT_CLIENT, "error: Failed to add sibling node, %s:%s, errno=%d", ip, port, errno);
            return NULL;
        }
        else
        {
            send_info( server, COMMAND_ADDSIBLINGNODE, COMMAND_ACTION_RETOUT_CLIENT, "info:New sibling node added, %s:%s", ip, port);
            return psiblingnodes[j];
        }
    }
    else
    {
        send_info( server, COMMAND_ADDSIBLINGNODE, COMMAND_ACTION_RETOUT_CLIENT, "error:Failed to add sibling node, too many nodes");
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
        log_info("Server connection %s:%d closed!\n", pservers[index]->ip, pservers[index]->port);
        free(pservers[index]);
        pservers[index] = NULL;
        return;
    }
    else if(ret < 0)
    {
        log_info("error recieve data, errno = %d\n", errno);
        return;
    }
    //ret > 0
    pservers[index]->bufferlength += ret;
    if(DEBUG)
    {
        log_info("handle_client_request[%d] buffer = \n", index);
        dumpbuffer(pservers[index]->buffer, pservers[index]->bufferlength);
    }

    if( pservers[index]->buffer[0] == COMMAND_QUIT)
    {
        close(pservers[index]->socket);
        log_info("Connection %s:%d closed!\n", pservers[index]->ip, pservers[index]->port);
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
            log_info("handle_server_request: server index = %d, start = \n", index);
            dumpbuffer(start, *(short*)(start + 2));
            dump_command_status(pservers[index]->firstcommand);
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
                	  pcommand->command.id == COMMAND_LISTDATABASE    ||
                	  pcommand->command.id == COMMAND_EXECUTEDDL      ||
                	  pcommand->command.id == COMMAND_EXECUTEDML      ||
                	  pcommand->command.id == COMMAND_EXECUTEDQL      ||
                	  pcommand->command.id == COMMAND_IMPORTCSV       ||
                	  pcommand->command.id == COMMAND_IMPORTCSX
                	  )
                {
                    pcommand->command.waitinginput = 1;
                    add_command(pservers[index], pcommand);
                    init_command(pcommand, start + 4, pservers[index]->socket);
                    send_info(pservers[index]->socket, pcommand->command.id, COMMAND_ACTION_RETSTART, NULL);
                }
                else
                {
                    log_info("error command %d\n", pcommand->command.id);
                    free(pcommand);
                }
            }
            else
            {
                log_info("the server is waiting for starting a command, but the input command action is %d\n", start[1]);
            }
        }
        else
        {   //waiting for input
            assert(pservers[index]->lastcommand != NULL && pservers[index]->lastcommand->command.waitinginput == 1);
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
                else if(pservers[index]->lastcommand->command.id == COMMAND_LISTDATABASE)	
                {
                    list_database(start + offset, pservers[index]->socket);
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
                    import_csv(start + offset, pservers[index]->socket, 0);
                }
                else if(pservers[index]->lastcommand->command.id == COMMAND_IMPORTCSX)
                {
                    import_csv(start + offset, pservers[index]->socket, 1);
                }
                else
                {
                    log_info("error request command=%d\n", pservers[index]->lastcommand->command.id);
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
                send_info(pservers[index]->socket, pservers[index]->lastcommand->command.id, COMMAND_ACTION_RETSTOP, "%ldm%ldus", sec, usec);
                
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
        //else
        //{
        //    printf("node is not ready to handle request\n");
        //    dump_command_status(pservers[index]->firstcommand);
        //}
        if(*(short*)(start+2) < 4)
        {
            log_info("error: wrong request, length=%d!", *(short*)(start+2));
            pservers[index]->bufferlength = 0;
            return;
        }
        else
        {
            i += *(short*)(start+2);
        }
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
    
    log_info("Listening server on %s:%s...\n", argv[2], argv[3]);
    log_info("Listening node on %s:%s...\n", argv[2], argv[4]);

    
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
            log_info("=============Loop started===============\n"	);
            log_info("server socket status:\n");
            for(i = 0; i < MAX_SERVER_COUNT; i++)
            {
                if(pservers[i] != NULL)
                {
                    log_info("pservers[%d](%s:%d)\n", i, pservers[i]->ip, pservers[i]->port);
                    dump_command_status(pservers[i]->firstcommand);
                }
            }
            log_info("sibling node socket status:\n");
            for(j = 0; j < MAX_NODE_COUNT; j++)
            {
                if(psiblingnodes[j] != NULL)
                {
                    log_info("psiblingnodes[%d](%s:%d)\n", j, psiblingnodes[j]->ip, psiblingnodes[j]->port);
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
                    log_info("select from psiblingnodes[%d]\n", i);
                }
            }
        }
        ret = select(max_fd + 1,&rdfs,NULL, NULL, NULL);

        //dump select result
        if(DEBUG)
        {
            log_info("select result:\n");
            for(i = 0; i < MAX_SERVER_COUNT; i++)
            {
                if(pservers[i]!= NULL && FD_ISSET(pservers[i]->socket, &rdfs))
                {
                    log_info("pservers[%d] is selected\n", i);;
                }
            }
            for(j = 0; j < MAX_NODE_COUNT; j++)
            {
                if(psiblingnodes[j] != NULL && FD_ISSET(psiblingnodes[j]->socket, &rdfs))
                {
                    log_info("childnode[%d] is selected\n", j);
                }
            }
        }

        if(ret < 0)  
        {  
            log_info("select error\n");  
        }
        else if(ret == 0)
        {
            log_info("time out\n");
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
                        log_info(outbuf, "error: failed to connect to the server, too many connections\n");
                        close(cli);
                    }
                    else
                    {
                        log_info("Connected to %s:%d\n",inet_ntoa(cliaddr.sin_addr),  
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
                        log_info(outbuf, "error:Failed to connect to sibling node, too many connections\n");
                        close(cli);
                    }
                    else
                    {
                        log_info("connect to sibling node %s:%d\n",inet_ntoa(cliaddr.sin_addr),  
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