/**
 * @file downstream.c
 *
 * @description This describes functions required to manage downstream messages.
 *
 * Copyright (c) 2015  Comcast
 */

#include "downstream.h"
#include "upstream.h" 
#include "connection.h"
#include "partners_check.h"
#include "ParodusInternal.h"
#define FALSE 0

#include <sys/ipc.h>
#include <sys/msg.h>

#define MSGPERM 0600    // msg queue permission
#define MSGTXTLEN 128   // msg text length

typedef struct mymsgbuf {              
  long    mtype;          /* Message type */
  reg_list_item_t *node;  // node where data needs to be sent 
  //char *  msg;            // holds the data to be sent 
  wrp_msg_t *message_sub;
  int msg_size;           // length of the data               
}reg_o_msg;

int actual_msgsz = sizeof(reg_o_msg) - sizeof(long);
int key, mask;
int msgid= 0;

void *push_message_function(  )
{

        char mock_client_dynamic[128] = {'\0'};
       // key = getuid();
       // mask = 0666;

        //msgid = msgget(key, mask | IPC_CREAT);
        msgid = msgget(IPC_PRIVATE, MSGPERM|IPC_CREAT|IPC_EXCL);
        if (msgid == -1) {
	  printf("Could not create message queue.\n");
        }
#if 1
	printf("create message queue. msg id : %d\n", msgid);
        struct mymsgbuf rcv; 
	char *argv[4]; 
        while (1) {

                if (msgrcv(msgid, &rcv, actual_msgsz, 0, 0)) {
                   // start webPA
			printf("DEBUG!!!!!!!!!! starting WebPA\n");
                        //system (webpa-dynamic, args);
                        argv[0] = rcv.node->url;
                        argv[1] = rcv.node->hw_mac;
                        sprintf(mock_client_dynamic,"./mock_client tcp://127.0.0.1:6666 %s %s & \n",argv[0],argv[1]);
			printf("DEBUG !!!!! mock_client_dynamic %s\n",mock_client_dynamic); 

                        //printf ( "recived msg %s , and the size %d " ,(rcv.msg), rcv.msg_size);
                        system (mock_client_dynamic);
                        // wait for webpa socket to be active. there is way to find out 
                        // get the scoket using mac and i/p
                        //printf( "msgid  : %d ,  actual_msgsz :%d rcv.msg %s rcv.msg_size %d  %s\n", msgid, actual_msgsz,rcv.msg,rcv.msg_size,__func__);  
                      
                   //TBD : need to find the right socket
                   ssize_t msg_len;
                   void *msg_bytes;
                   msg_len = wrp_struct_to (rcv.message_sub, WRP_BYTES,&msg_bytes) ;

                   sleep(1);
  
                   //ParodusPrint("%s, sending down stream on sock:%d \n",__func__,rcv.node->sock);
                   //int bytes = nn_send(rcv.node->sock, rcv.msg, rcv.msg_size, NN_DONTWAIT);
                   //ParodusPrint("downstream bytes sent:%d\n", bytes);
                   listenerOnMessage(msg_bytes, msg_len);

                }
                memset(&rcv,0,sizeof(reg_o_msg));

        }
#endif
        //printf("DEBUG !!!!! push_msg_func %d\n",msgid);
        return 0;
}

void StartPThread(void)
{
        int err = 0;
        pthread_t threadId;

        err = pthread_create(&threadId, NULL, push_message_function, NULL);
        if (err != 0)
        {
                ParodusError("Error creating thread :[%s]\n", strerror(err));
        exit(1);
        }
        else
        {
                ParodusPrint("Thread created Successfully %d\n", (int ) threadId);
        }
}

/*----------------------------------------------------------------------------*/
/*                             External Functions                             */
/*----------------------------------------------------------------------------*/

/**
 * @brief listenerOnMessage function to create WebSocket listener to receive connections
 *
 * @param[in] msg The message received from server for various process requests
 * @param[in] msgSize message size
 */
void listenerOnMessage(void * msg, size_t msgSize)
{
    int rv =0;
    wrp_msg_t *message;
    wrp_msg_t *message_sub;
    char* destVal = NULL;
    char dest[32] = {'\0'};
    int msgType;
    int bytes =0;
    int destFlag =0;
    size_t size = 0;
    int resp_size = -1 ;
    const char *recivedMsg = NULL;
    char *str= NULL;
    wrp_msg_t *resp_msg = NULL;
    void *resp_bytes;
    cJSON *response = NULL;
    //reg_list_item_t *temp = NULL;
    reg_list_item_t *curr_node = NULL; // newly added
    //char *service_name = NULL;
    recivedMsg =  (const char *) msg;

    ParodusInfo("Received msg from server:%s\n", recivedMsg);
    if(recivedMsg!=NULL) 
    {
        /*** Decoding downstream recivedMsg to check destination ***/
        rv = wrp_to_struct(recivedMsg, msgSize, WRP_BYTES, &message);

        if(rv > 0)
        {
            ParodusPrint("\nDecoded recivedMsg of size:%d\n", rv);
            msgType = message->msg_type;
            ParodusInfo("msgType received:%d\n", msgType);

            if(message->msg_type == WRP_MSG_TYPE__REQ)
            {
                ParodusPrint("numOfClients registered is %d\n", get_numOfClients());
                int ret = validate_partner_id(message, NULL);
                if(ret < 0)
                {
                    response = cJSON_CreateObject();
                    cJSON_AddNumberToObject(response, "statusCode", 430);
                    cJSON_AddStringToObject(response, "message", "Invalid partner_id");
                }

                if((message->u.req.dest !=NULL) && (ret >= 0))
                {

               	    // ensure socket is available with the above ip and mac
                    // may required to launch WebPA with above parameters if not exist
               	    // For now , findout out the right scoket based on the mac/ip
                    destVal = message->u.req.dest;
                    //service_name = message->u.req.dest;
                    strtok(destVal , "/");
                    parStrncpy(dest,strtok(NULL , "/"), sizeof(dest));
                    ParodusInfo("Received downstream dest as :%s @@@@@@@@\n", dest);
                    ParodusInfo("Received downstream destVal as :%s @@@@@@@@@@@\n", destVal);
                    //temp = get_global_node();
                    curr_node = get_curr_node(destVal,dest);
                    //Checking for individual clients & Sending to each client

                    if(curr_node  != NULL && curr_node->sock > 0 )
                    {
                        //ParodusPrint("node is pointing to temp->service_name %s \n",temp->service_name);
                        // Sending message to registered clients
                        //if( strcmp(dest, temp->service_name) == 0)
                        {
                            ParodusPrint("sending to nanomsg client %s\n", dest);
                            bytes = nn_send(curr_node->sock, recivedMsg, msgSize, NN_DONTWAIT);
                            ParodusInfo("sent downstream message '%s' to reg_client '%s'\n",recivedMsg,curr_node->url);
                            ParodusPrint("downstream bytes sent:%d\n", bytes);
                            destFlag =1;
                            //break;
                        }
                        ParodusPrint("checking the next item in the list\n");
                        //temp= temp->next;
                    }
                    else if (curr_node != NULL && curr_node->sock == 0){
                        printf( " WARNING !! start WEBPA   \n");
#if 1  // commented for now as there is problem seen while sending recivedMsg  through msg queue 
                        struct mymsgbuf reg_msg;
                        wrp_to_struct(recivedMsg, msgSize, WRP_BYTES, &message_sub);

                        // fill struct members of reg_msg
                        reg_msg. mtype   = 1;          /* Message type */
                        reg_msg.node = curr_node;  // node where data needs to be sent 
                        //reg_msg.msg = (char *)recivedMsg;            // holds the data to be sent 
                        reg_msg.msg_size = msgSize;           // length of the data               
                        reg_msg.message_sub = message_sub;           // length of the data               
                        printf( "msgid  : %d ,  actual_msgsz :%d and message recivedMsg %s , \n", msgid, actual_msgsz,recivedMsg);  
                	int ret = msgsnd(msgid, &reg_msg, actual_msgsz, 0);
#endif 
#if 0 
                        char * argv[4];
                        char mock_client_dynamic[128] = {'\0'};
                        argv[0] = curr_node->url;
                        argv[1] = curr_node->hw_mac;
			//need to copy mock client binary to the same location of parodus binary 
                        sprintf(mock_client_dynamic,"./mock_client tcp://127.0.0.1:6666 %s %s & \n",argv[0],argv[1]);
                        printf("DEBUG !!!!! mock_client_dynamic %s\n",mock_client_dynamic);
                        // webPA launch 
                        system (mock_client_dynamic);
                        sleep(3);   // added sleep for registration to happen and sock creation
                        ParodusPrint("sending to nanomsg client %s\n", dest);
                        bytes = nn_send(curr_node->sock, recivedMsg, msgSize, NN_DONTWAIT);
                        ParodusInfo("sent downstream message '%s' to reg_client '%s'\n",recivedMsg,curr_node->url);
                        ParodusPrint("downstream bytes sent:%d\n", bytes);
#endif 
                        destFlag =1;

                        printf("WARNING!!! return value is %d %d with err : %s\n",ret,__LINE__, strerror(errno));
                    }
                    else {
                        printf( "WARNING  CLIENT is not registered !!!!!   \n"); 
                    	// Required to launch WebPA with above ip and mac
                    }

                    //if any unknown dest received sending error response to server
                    if(destFlag ==0)
                    {
                        ParodusError("Unknown dest:%s\n", dest);
                        response = cJSON_CreateObject();
                        cJSON_AddNumberToObject(response, "statusCode", 531);
                        cJSON_AddStringToObject(response, "message", "Service Unavailable");
                    }
                }

                if(destFlag == 0 || ret < 0)
                {
                    resp_msg = (wrp_msg_t *)malloc(sizeof(wrp_msg_t));
                    memset(resp_msg, 0, sizeof(wrp_msg_t));

                    resp_msg ->msg_type = msgType;
                    resp_msg ->u.req.source = message->u.req.dest;
                    resp_msg ->u.req.dest = message->u.req.source;
                    resp_msg ->u.req.transaction_uuid=message->u.req.transaction_uuid;

                    if(response != NULL)
                    {
                        str = cJSON_PrintUnformatted(response);
                        ParodusInfo("Payload Response: %s\n", str);

                        resp_msg ->u.req.payload = (void *)str;
                        resp_msg ->u.req.payload_size = strlen(str);

                        ParodusPrint("msgpack encode\n");
                        resp_size = wrp_struct_to( resp_msg, WRP_BYTES, &resp_bytes );
                        if(resp_size > 0)
                        {
                            size = (size_t) resp_size;
                            sendUpstreamMsgToServer(&resp_bytes, size);
                        }
                        free(str);
                        cJSON_Delete(response);
                        free(resp_bytes);
                        resp_bytes = NULL;
                    }
                    free(resp_msg);
                }
            }
        }
        else
        {
            ParodusError( "Failure in msgpack decoding for receivdMsg: rv is %d\n", rv );
        }
        ParodusPrint("free for downstream decoded msg\n");
        wrp_free_struct(message);
    }
}
