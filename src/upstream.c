/**
 * @file upstream.c
 *
 * @description This describes functions required to manage upstream messages.
 *
 * Copyright (c) 2015  Comcast
 */

#include "ParodusInternal.h"
#include "upstream.h"
#include "config.h"
#include "partners_check.h"
#include "connection.h"
#include "client_list.h"
#include "nopoll_helpers.h"
#include "conn_interface.h"

/*----------------------------------------------------------------------------*/
/*                                   Macros                                   */
/*----------------------------------------------------------------------------*/
#define METADATA_COUNT 					12

/*----------------------------------------------------------------------------*/
/*                            File Scoped Variables                           */
/*----------------------------------------------------------------------------*/
ParodusCfg parodusCfg;
void *metadataPack;
size_t metaPackSize=-1;


UpStreamMsg *UpStreamMsgQ = NULL;

pthread_mutex_t nano_mut=PTHREAD_MUTEX_INITIALIZER;

pthread_cond_t nano_con=PTHREAD_COND_INITIALIZER;

/*----------------------------------------------------------------------------*/
/*                             Internal Functions                             */
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
/*                             External functions                             */
/*----------------------------------------------------------------------------*/
void set_para_config(ParodusCfg* parodusCfg_tar)
{
   // should return paraconfig
   printf(" sekhar entring .. memeset %s \n", __func__);
   memcpy(parodusCfg_tar, &parodusCfg, sizeof(ParodusCfg));
   printf(" sekhar done .. memeset %s \n", __func__);
   return;
}
void packMetaData()
{
    char boot_time[256]={'\0'};
    //Pack the metadata initially to reuse for every upstream msg sending to server
    ParodusPrint("-------------- Packing metadata ----------------\n");
    sprintf(boot_time, "%d", get_parodus_cfg()->boot_time);

    struct data meta_pack[METADATA_COUNT] = {
            {HW_MODELNAME, get_parodus_cfg()->hw_model},
            {HW_SERIALNUMBER, get_parodus_cfg()->hw_serial_number},
            {HW_MANUFACTURER, get_parodus_cfg()->hw_manufacturer},
            {HW_DEVICEMAC, get_parodus_cfg()->hw_mac},
            {HW_LAST_REBOOT_REASON, get_parodus_cfg()->hw_last_reboot_reason},
            {FIRMWARE_NAME , get_parodus_cfg()->fw_name},
            {BOOT_TIME, boot_time},
            {LAST_RECONNECT_REASON, get_global_reconnect_reason()},
            {WEBPA_PROTOCOL, get_parodus_cfg()->webpa_protocol},
            {WEBPA_UUID,get_parodus_cfg()->webpa_uuid},
            {WEBPA_INTERFACE, get_parodus_cfg()->webpa_interface_used},
            {PARTNER_ID, get_parodus_cfg()->partner_id}
        };

    const data_t metapack = {METADATA_COUNT, meta_pack};

    metaPackSize = wrp_pack_metadata( &metapack , &metadataPack );

    if (metaPackSize > 0) 
    {
	    ParodusPrint("metadata encoding is successful with size %zu\n", metaPackSize);
    }
    else
    {
	    ParodusError("Failed to encode metadata\n");
    }
}
       
/*
 * @brief To handle UpStream messages which is received from nanomsg server socket
 */

void *handle_upstream()
{
    UpStreamMsg *message;
    int sock, bind;
    int bytes =0;
    void *buf;

    ParodusPrint("******** Start of handle_upstream ******** at %d , %s \n", __LINE__,__func__);

    sock = nn_socket( AF_SP, NN_PULL );
    if(sock >= 0)
    {
        ParodusPrint("Nanomsg bind with get_parodus_cfg()->local_url  %s\n", get_parodus_cfg()->local_url);
        bind = nn_bind(sock, get_parodus_cfg()->local_url);
        if(bind < 0)
        {
            ParodusError("Unable to bind socket (errno=%d, %s)\n",errno, strerror(errno));
        }
        else
        {
            while( FOREVER() ) 
            {
                buf = NULL;
                ParodusInfo("nanomsg server gone into the listening mode...at %d , %s \n", __LINE__,__func__);
                bytes = nn_recv (sock, &buf, NN_MSG, 0);
                ParodusInfo ("Upstream message received from nanomsg client: \"%s\"\n", (char*)buf);
                message = (UpStreamMsg *)malloc(sizeof(UpStreamMsg));

                if(message)
                {
                    ParodusInfo("nanomsg server msg recvd ...at %d , %s \n", __LINE__,__func__);
                    message->msg =buf;
                    message->len =bytes;
                    message->next=NULL;
                    ParodusInfo("waiting for lock..at %d , %s \n", __LINE__,__func__);
                    pthread_mutex_lock (&nano_mut);
                    //Producer adds the nanoMsg into queue
                    if(UpStreamMsgQ == NULL)
                    {
                        ParodusInfo("Queue is NULL..at %d , %s \n", __LINE__,__func__);
                        UpStreamMsgQ = message;

                        ParodusPrint("Producer added messageat %d , %s \n", __LINE__,__func__);
                        pthread_cond_signal(&nano_con);
                        pthread_mutex_unlock (&nano_mut);
                        ParodusPrint("mutex unlock in producer threadat %d , %s \n", __LINE__,__func__);
                    }
                    else
                    {
                        ParodusInfo("else pushing the msg .at %d , %s \n", __LINE__,__func__);
                        UpStreamMsg *temp = UpStreamMsgQ;
                        while(temp->next)
                        {
                            temp = temp->next;
                        }
                        temp->next = message;
                        pthread_mutex_unlock (&nano_mut);
                    }
                    ParodusInfo("got lock and proces un locked  ..at %d , %s \n", __LINE__,__func__);
                }
                else
                {
                    ParodusError("failure in allocation for message at %d , %s \n", __LINE__,__func__);
                }
            }
        }
    }
    else
    {
        ParodusError("Unable to create socket (errno=%d, %s)\n",errno, strerror(errno));
    }
    ParodusPrint ("End of handle_upstream at %d , %s \n", __LINE__,__func__);
    return 0;
}


void *processUpstreamMessage()
{		
    int rv=-1;
    // rc = -1;	
    int msgType;
    wrp_msg_t *msg;	
    void *appendData, *bytes;
    size_t encodedSize;
    reg_list_item_t *temp = NULL;
    //int matchFlag = 0;
    int status = -1;

    while(FOREVER())
    {
        pthread_mutex_lock (&nano_mut);
        ParodusPrint("mutex lock in consumer thread at %d , %s \n", __LINE__,__func__);
        if(UpStreamMsgQ != NULL)
        {
            UpStreamMsg *message = UpStreamMsgQ;
            UpStreamMsgQ = UpStreamMsgQ->next;
            if(UpStreamMsgQ == NULL)
                 ParodusInfo("WARNING !!!!  UpStreamMsgQ  is NULL..at %d , %s \n", __LINE__,__func__);

            pthread_mutex_unlock (&nano_mut);
            ParodusPrint("mutex unlock in consumer thread at %d , %s \n", __LINE__,__func__);

            /*** Decoding Upstream Msg to check msgType ***/
            /*** For MsgType 9 Perform Nanomsg client Registration else Send to server ***/	
            ParodusPrint("---- Decoding Upstream Msg ---- at %d , %s \n", __LINE__,__func__ );

            rv = wrp_to_struct( message->msg, message->len, WRP_BYTES, &msg );
            if(rv > 0)
            {
                msgType = msg->msg_type;				   
                if(msgType == 9)
                {
                    ParodusInfo("\n Nanomsg client Registration for Upstream at %d , %s \n", __LINE__,__func__);
#if 0
                    // create soc connetion with webserver based on i/p .... TBD hard code MAC for now
                    ParodusCfg parodusCfg;
                    get_para_config(&parodusCfg);
                    if (strstr(msg->u.reg.url,"6667") )
                    	 strcpy(parodusCfg.hw_mac, "48fc01fb1081"); // use 1 st MAC
                    else
                    	strcpy(parodusCfg.hw_mac, "48fc01fb1091"); // use 2nd MAC
                    get paraconfig and replace mac id
#endif

                    ParodusInfo("\n going to create connection at %d , %s \n", __LINE__,__func__);
                    //createSocketConnection(&parodusCfg,NULL);
                    // add to the list .. so that we can track it for serving upcoming requests
                    ParodusInfo("\n  going to add it to the list at %d , %s \n", __LINE__,__func__);
                    status = addToList(&msg);

                    ParodusInfo("\n going to call nn_scoket  at %d , %s  prev stat : %d \n", __LINE__,__func__,status);
		    temp = get_global_node();
#if 0
                    ParodusInfo("\n get_global_node ...   at %d , %s \n", __LINE__,__func__);
                    //just create soc with nano msging server
                    temp->sock = nn_socket(AF_SP,NN_PUSH );
                    printf ("%s, updating sock value to :%d for mac : %s \n", __func__,temp->sock,temp->hw_mac );
#endif
                    if(temp->sock >= 0)
                    {
#if 0
                       int t = NANOMSG_SOCKET_TIMEOUT_MSEC;
                        ParodusInfo("\n going to call nn_isett_ scoket  at %d , %s \n", __LINE__,__func__);
                       rc = nn_setsockopt(temp->sock, NN_SOL_SOCKET, NN_SNDTIMEO, &t, sizeof(t));
                       if(rc < 0)
                       {
                          ParodusError ("Unable to set socket timeout (errno=%d, %s)\n",errno, strerror(errno));
                          ParodusError ("Unable to set socket timeout at %d , %s \n", __LINE__,__func__);
                       }
                       ParodusInfo("\n going to call nn_connect  at %d , %s \n", __LINE__,__func__);
                       rc = nn_connect(temp->sock, msg->u.reg.url);
                       if(rc < 0)
                       {
                          ParodusError ("Unable to connect socket at %d , %s \n", __LINE__,__func__);
                       }
                       else
                       {
                          ParodusInfo("Client registered before. Sending acknowledgement at %d , %s \n", __LINE__,__func__);
                          status =sendAuthStatus(temp);
                          if(status == 0)
                          {
                              ParodusPrint("sent auth status to reg client at %d , %s \n", __LINE__,__func__);
                          }
                       }
                       ParodusInfo("\n FINISHED !!!!! scoket  at %d , %s \n", __LINE__,__func__);
                       ParodusPrint("sent auth status to reg client at %d , %s \n", __LINE__,__func__);
#endif
                    }
                    else
                    {
                       ParodusError("Unable to create socket (errno=%d, %s)\n",errno, strerror(errno));
                       ParodusError("Unable to create socket at %d , %s \n", __LINE__,__func__);
                    }
                }
                else if(msgType == WRP_MSG_TYPE__EVENT)
                {
                    ParodusInfo(" Received upstream event data at %d , %s \n", __LINE__,__func__);
                    partners_t *partnersList = NULL;

                    int ret = validate_partner_id(msg, &partnersList);
                    if(ret == 1)
                    {
                        wrp_msg_t *eventMsg = (wrp_msg_t *) malloc(sizeof(wrp_msg_t));
                        eventMsg->msg_type = msgType;
                        eventMsg->u.event.content_type=msg->u.event.content_type;
                        eventMsg->u.event.source=msg->u.event.source;
                        eventMsg->u.event.dest=msg->u.event.dest;
                        eventMsg->u.event.payload=msg->u.event.payload;
                        eventMsg->u.event.payload_size=msg->u.event.payload_size;
                        eventMsg->u.event.headers=msg->u.event.headers;
                        eventMsg->u.event.metadata=msg->u.event.metadata;
                        eventMsg->u.event.partner_ids = partnersList;

                        int size = wrp_struct_to( eventMsg, WRP_BYTES, &bytes );
                        if(size > 0)
                        {
                            sendUpstreamMsgToServer(&bytes, size);
                        }
                        free(eventMsg);
                        free(bytes);
                        bytes = NULL;
                    }
                    else
                    {
                        sendUpstreamMsgToServer(&message->msg, message->len);
                    }
                }
                else
                {
                    //Sending to server for msgTypes 3, 5, 6, 7, 8.
                    ParodusInfo(" Received upstream data with MsgType: %d\n", msgType);
                    //Appending metadata with packed msg received from client
                    if(metaPackSize > 0)
                    {
                        ParodusPrint("Appending received msg with metadata\n");
                        encodedSize = appendEncodedData( &appendData, message->msg, message->len, metadataPack, metaPackSize );
                        ParodusPrint("encodedSize after appending :%zu\n", encodedSize);
                        ParodusPrint("metadata appended upstream msg %s\n", (char *)appendData);
                        ParodusInfo("Sending metadata appended upstream msg to server at %d , %s \n", __LINE__,__func__);
                        sendMessage(get_global_conn(),appendData, encodedSize);

                        free( appendData);
                        appendData =NULL;
                    }
                    else
                    {		
                        ParodusError("Failed to send upstream as metadata packing is not successful at %d , %s \n", __LINE__,__func__);
                    }
                }
            }
            else
            {
                ParodusError("Erpthread_cond_waitror in msgpack decoding for upstream at %d , %s \n", __LINE__,__func__);
            }
            ParodusPrint("Free for upstream decoded msg at %d , %s \n", __LINE__,__func__);
            wrp_free_struct(msg);
            msg = NULL;

            if(nn_freemsg (message->msg) < 0)
            {
                ParodusError ("Failed to free msg at %d , %s \n", __LINE__,__func__);
            }
            free(message);
            message = NULL;
        }
        else
        {
            ParodusPrint("Before pthread cond wait in consumer thread at %d , %s \n", __LINE__,__func__);   
            pthread_cond_wait(&nano_con, &nano_mut);
            pthread_mutex_unlock (&nano_mut);
            ParodusPrint("mutex unlock in consumer thread after cond wait at %d , %s \n", __LINE__,__func__);
        }
    }
   
    return NULL;
}

void sendUpstreamMsgToServer(void **resp_bytes, size_t resp_size)
{
	void *appendData;
	size_t encodedSize;

	//appending response with metadata 			
	if(metaPackSize > 0)
	{
	   	encodedSize = appendEncodedData( &appendData, *resp_bytes, resp_size, metadataPack, metaPackSize );
	   	ParodusPrint("metadata appended upstream response %s\n", (char *)appendData);
	   	ParodusPrint("encodedSize after appending :%zu\n", encodedSize);
	   		   
		ParodusInfo("Sending response to server\n");
	   	sendMessage(get_global_conn(),appendData, encodedSize);
	   	
		free(appendData);
		appendData =NULL;
	}
	else
	{		
		ParodusError("Failed to send upstream as metadata packing is not successful\n");
	}

}
