#define COMMAND_QUIT           0
#define COMMAND_ADDCHILDNODE   1
#define COMMAND_ADDSIBLINGNODE 2
#define COMMAND_GETNODEINFO    3
#define COMMAND_CREATEDATABASE 4
#define COMMAND_SETDATABASE    5
#define COMMAND_EXECUTEDDL     6
#define COMMAND_EXECUTEDML     7
#define COMMAND_EXECUTEDQL     8
#define COMMAND_IMPORTCSV      9

#define COMMAND_ACTION_EXESTART         1
#define COMMAND_ACTION_EXEINPUT_ONE     2
#define COMMAND_ACTION_EXEINPUT         3
#define COMMAND_ACTION_EXESTOP          4
#define COMMAND_ACTION_RETSTART         5
#define COMMAND_ACTION_RETOUTPUT        6
#define COMMAND_ACTION_RETSTOP          7

struct doom_dh_command
{
    char id;
    int  no;               //command serial number
    int  parentno;         //parent command's serial number
    int  waitinginput;
    int  waitingresult;
    struct timeval starttime;
};

struct doom_dh_command_node
{
    struct doom_dh_command command;
    struct doom_dh_command_node *prev, *next;
};

void dump_command_status(struct doom_dh_command_node *pcommand)
{
    while(pcommand != NULL)
    {
        printf("Command %d: no=%d,parentno=%d, waitinginput=%d, waitingresult=%d\n", 
                pcommand->command.id, pcommand->command.no, pcommand->command.parentno, pcommand->command.waitinginput, pcommand->command.waitingresult);
        pcommand = pcommand->next;
    }
};
