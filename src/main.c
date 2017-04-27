/**
 * Copyright 2016 Comcast Cable Communications Management, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
 
#include <string.h>

#include "config.h"
#include "conn_interface.h"
#include "parodus_log.h"
#include "spin_thread.h"
#include "ParodusInternal.h"
#include "service_alive.h"
#include "thread_tasks.h"
#include "upstream.h"
#include "downstream.h"
#include <pthread.h>
/*----------------------------------------------------------------------------*/
/*                                   Macros                                   */
/*----------------------------------------------------------------------------*/
/* none */

/*----------------------------------------------------------------------------*/
/*                               Data Structures                              */
/*----------------------------------------------------------------------------*/
/* none */

/*----------------------------------------------------------------------------*/
/*                            File Scoped Variables                           */
/*----------------------------------------------------------------------------*/
/* none */

/*----------------------------------------------------------------------------*/
/*                             Function Prototypes                            */
/*----------------------------------------------------------------------------*/
/* none */

/*----------------------------------------------------------------------------*/
/*                             External Functions                             */
/*----------------------------------------------------------------------------*/
#if 0 // created for testing purpose
int socket_listen()
{
    int socket_desc , client_sock , c , read_size;
    struct sockaddr_in server , client;
    char client_message[12];

    //Create socket
    socket_desc = socket(AF_INET , SOCK_STREAM , 0);
    if (socket_desc == -1)
    {
        printf("Could not create socket");
    }
    puts("Socket created");

    //Prepare the sockaddr_in structure
    server.sin_family = AF_INET;
    server.sin_addr.s_addr = INADDR_ANY;
    server.sin_port = htons( 8889 );

    //Bind
    if( bind(socket_desc,(struct sockaddr *)&server , sizeof(server)) < 0)
    {
        //print the error message
        perror("bind failed. Error");
        return 1;
    }
    puts("bind done");

    //Listen
    listen(socket_desc , 3);

    //Accept and incoming connection
    puts("Waiting for incoming connections...");
    c = sizeof(struct sockaddr_in);

    //accept connection from an incoming client
    client_sock = accept(socket_desc, (struct sockaddr *)&client, (socklen_t*)&c);
    if (client_sock < 0)
    {
        perror("accept failed");
        return 1;
    }
    puts("Connection accepted");

    //Receive a message from client
    if( (read_size = recv(client_sock , client_message , 12 , 0)) > 0 )
         printf("Client to server msg is %s\n",client_message);

    if(read_size == 0)
    {
        puts("Client disconnected");
        fflush(stdout);
    }
    else if(read_size == -1)
    {
        perror("recv failed");
    }
    return 0;
}
#endif
ParodusCfg parodusCfg;
int main( int argc, char **argv)
{
    memset(&parodusCfg,0,sizeof(parodusCfg));
    
    ParodusInfo("********** Starting component: Parodus **********\n "); 
    parseCommandLine(argc,argv,&parodusCfg);
    UpStreamMsgQ = NULL;
    StartPThread();
    StartThread(handle_upstream);
    StartThread(processUpstreamMessage);
    ParodusMsgQ = NULL;
    StartThread(messageHandlerTask);
    StartThread(serviceAliveTask);
    set_para_config(&parodusCfg);
    createSocketConnection(&parodusCfg,NULL);

#if 0
    // create a listener for webserver requests for testing
    int socket_ret = socket_listen();
    if(socket_ret == 1)
    	printf("Socket: failed");
     Upon receiving request ... pass it on to the right webPA agent
#endif
    //while(1) sleep (5);
    return 0;
}

const char *rdk_logger_module_fetch(void)
{
    return "LOG.RDK.PARODUS";
}

/*----------------------------------------------------------------------------*/
/*                             Internal functions                             */
/*----------------------------------------------------------------------------*/
/* none */
