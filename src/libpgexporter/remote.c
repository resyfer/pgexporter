/*
 * Copyright (C) 2023 Red Hat
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or other
 * materials provided with the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its contributors may
 * be used to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
 * OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
 * TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/* pgexporter */
#include <pgexporter.h>
#include <logging.h>
#include <management.h>
#include <memory.h>
#include <message.h>
#include <network.h>
#include <remote.h>
#include <security.h>
#include <utils.h>

/* system */
#include <ev.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>

void
pgexporter_remote_management(int client_fd, char* address)
{
   int server_fd = -1;
   int status;
   int exit_code;
   int auth_status;
   signed char type;
   SSL* client_ssl = NULL;
   struct message* msg = NULL;
   struct configuration* config;

   pgexporter_start_logging();
   pgexporter_memory_init();

   exit_code = 0;

   config = (struct configuration*)shmem;

   pgexporter_log_debug("pgexporter_remote_management: connect %d", client_fd);

   auth_status = pgexporter_remote_management_auth(client_fd, address, &client_ssl);
   if (auth_status == AUTH_SUCCESS)
   {
      status = pgexporter_read_timeout_message(client_ssl, client_fd, 5 /* TODO config->authentication_timeout */, &msg);
      if (status != MESSAGE_STATUS_OK)
      {
         goto done;
      }

      type = pgexporter_read_byte(msg->data);

      if (pgexporter_connect_unix_socket(config->unix_socket_dir, MAIN_UDS, &server_fd))
      {
         goto done;
      }

      status = pgexporter_write_message(NULL, server_fd, msg);
      if (status != MESSAGE_STATUS_OK)
      {
         goto done;
      }

      switch (type)
      {
         case MANAGEMENT_STOP:
         case MANAGEMENT_RESET:
         case MANAGEMENT_RELOAD:
            break;
         case MANAGEMENT_STATUS:
         case MANAGEMENT_ISALIVE:
         case MANAGEMENT_DETAILS:
            do
            {
               status = pgexporter_read_timeout_message(NULL, server_fd, 1, &msg);
               if (status != MESSAGE_STATUS_OK)
               {
                  goto done;
               }

               status = pgexporter_write_message(client_ssl, client_fd, msg);
            }
            while (status == MESSAGE_STATUS_OK);
            break;
         default:
            pgexporter_log_warn("Unknown management operation: %d", type);
            exit_code = 1;
            goto done;
            break;
      }
   }
   else
   {
      exit_code = 1;
   }

done:

   if (client_ssl != NULL)
   {
      int res;
      SSL_CTX* ctx = SSL_get_SSL_CTX(client_ssl);
      res = SSL_shutdown(client_ssl);
      if (res == 0)
      {
         SSL_shutdown(client_ssl);
      }
      SSL_free(client_ssl);
      SSL_CTX_free(ctx);
   }

   pgexporter_log_debug("pgexporter_remote_management: disconnect %d", client_fd);
   pgexporter_disconnect(client_fd);
   pgexporter_disconnect(server_fd);

   free(address);

   pgexporter_memory_destroy();
   pgexporter_stop_logging();

   exit(exit_code);
}
