/*
 * Copyright (C) 2025 The pgexporter community
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

#include <pgexporter.h>
#include <art.h>
#include <deque.h>
#include <http.h>
#include <logging.h>
#include <prometheus_client.h>
#include <utils.h>
#include <value.h>

#include <errno.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

static int prometheus_metric_find_create(struct prometheus_bridge* bridge, char* name, struct prometheus_metric** metric);
static int prometheus_metric_set_name(struct prometheus_metric* metric, char* name);
static int prometheus_metric_set_help(struct prometheus_metric* metric, char* help);
static int prometheus_metric_set_type(struct prometheus_metric* metric, char* type);
static int prometheus_add_definition(struct prometheus_metric* metric, struct deque** attr, struct deque** val);
static int prometheus_add_attribute(struct deque** attributes, const char* const key, const char* const value);
static int prometheus_add_value(struct deque** values, const char* const value);
static int prometheus_add_line(struct prometheus_metric* metric, char* line, const char* endpoint);
static int prometheus_parse_bridge_body(const char* endpoint, char* body, struct prometheus_bridge* bridge);
static int parse_metric_line(struct prometheus_metric* metric, struct deque** attrs, struct deque** vals, const char* line, const char* endpoint);
static char* endpoint_to_url(struct endpoint* e);

int
pgexporter_prometheus_client_create_bridge(struct prometheus_bridge** bridge)
{
   struct prometheus_bridge* b = NULL;

   *bridge = NULL;

   b = (struct prometheus_bridge*)malloc(sizeof(struct prometheus_bridge));

   if (b == NULL)
   {
      pgexporter_log_error("Failed to allocate bridge");
      goto error;
   }

   memset(b, 0, sizeof(struct prometheus_bridge));

   if (pgexporter_art_create(&b->metrics))
   {
      pgexporter_log_error("Failed to create ART");
      goto error;
   }

   *bridge = b;

   return 0;

error:

   return 1;
}

int
pgexporter_prometheus_client_destroy_bridge(struct prometheus_bridge* bridge)
{
   if (bridge != NULL)
   {
      pgexporter_art_destroy(bridge->metrics);
   }

   free(bridge);

   return 0;
}

int
pgexporter_prometheus_client_get(struct endpoint* endpoint, struct prometheus_bridge* bridge)
{
   struct http* http = NULL;
   char* url = NULL;

   url = endpoint_to_url(endpoint);
   if (url == NULL)
   {
      goto error;
   }

   if (pgexporter_http_create(url, &http))
   {
      pgexporter_log_error("Failed to create HTTP interaction with %s", url);
   }
   {
      goto error;
   }

   if (pgexporter_http_get(http))
   {
      pgexporter_log_error("Failed to execute HTTP/GET interaction with %s", url);
      goto error;
   }

   pgexporter_http_log(http);

   // The attributes and values are copied over.
   if (prometheus_parse_bridge_body(url, http->body, bridge))
   {
      // TODO: What do we do with the bridge?
      goto error;
   }

   pgexporter_http_destroy(http);

   return 0;

error:

   pgexporter_http_destroy(http);

   return 1;
}

static int
prometheus_metric_find_create(struct prometheus_bridge* bridge, char* name, struct prometheus_metric** metric)
{
   struct prometheus_metric* m = NULL;

   *metric = NULL;

   m = (struct prometheus_metric*)pgexporter_art_search(bridge->metrics, (unsigned char*)name, strlen(name));

   if (m == NULL)
   {
      struct deque* defs = NULL;

      m = (struct prometheus_metric*)malloc(sizeof(struct prometheus_metric));
      memset(m, 0, sizeof(struct prometheus_metric));

      if (pgexporter_deque_create(true, &defs))
      {
         goto error;
      }

      m->name = strdup(name);
      m->definitions = defs;

      if (pgexporter_art_insert(bridge->metrics, (unsigned char*)name, strlen(name),
                                (uintptr_t)m, ValueRef))
      {
         goto error;
      }
   }

   *metric = m;

   return 0;

error:

   return 1;
}

static int
prometheus_metric_set_name(struct prometheus_metric* metric, char* name)
{
   if (metric->name != NULL)
   {
      /* Assumes information is already present. */
      return 0;
   }

   metric->name = strdup(name);
   if (metric->name == NULL)
   {
      errno = 0;
      return 1;
   }

   return 0;
}

static int
prometheus_metric_set_help(struct prometheus_metric* metric, char* help)
{
   if (metric->help != NULL)
   {
      free(metric->help);
      metric->help = NULL;
   }

   metric->help = strdup(help);

   if (metric->help == NULL)
   {
      errno = 0;
      return 1;
   }

   return 0;
}

static int
prometheus_metric_set_type(struct prometheus_metric* metric, char* type)
{
   if (metric->type != NULL)
   {
      free(metric->type);
      metric->type = NULL;
   }

   metric->type = strdup(type);

   if (metric->type == NULL)
   {
      errno = 0;
      return 1;
   }

   return 0;
}

static int
parse_metric_line(struct prometheus_metric* metric, struct deque** attrs,
                  struct deque** vals, const char* line, const char* endpoint)
{
   /* Metric lines are the ones that actually carry metrics, and not help or type. */

   char key[MISC_LENGTH] = {0};
   char value[MISC_LENGTH] = {0};
   char* token = NULL;
   char* saveptr = NULL;
   char* line_cpy = NULL;

   if (line == NULL)
   {
      goto error;
   }

   line_cpy = strdup(line); /* strtok modifies the string. */
   if (line_cpy == NULL)
   {
      goto error;
   }

   /* Lines of the form:
    *
    * type{key1="value1",key2="value2",...} value
    *
    * Tokenizing on a " " will give the value on second call.
    * The first token can be further tokenized on "{,}".
    **/

   token = strtok_r(line_cpy, " ", &saveptr);

   while (token != NULL)
   {
      if (token == line_cpy)
      {
         /* First token is the name of the metric. So just a sanity check. */

         if (strcmp(token, metric->name))
         {
            goto error;
         }
      }
      else if (*saveptr == '\0')
      {
         /* Final token. */

         if (prometheus_add_value(vals, token))
         {
            /* TODO: Clear memory of items in deque. */
            goto error;
         }
      }
      else if (strlen(token) > 0)
      {
         /* Assuming of the form key="value" */

         sscanf(token, "%127s=\"%127s\"", key, value);

         if (strlen(key) == 0 || strlen(value) == 0)
         {
            goto error;
         }

         if (prometheus_add_attribute(attrs, key, value))
         {
            /* TODO: Clear memory of items in deque. */
            goto error;
         }
      }

      token = strtok_r(NULL, " ", &saveptr);
   }

   if (prometheus_add_attribute(attrs, "endpoint", endpoint))
   {
      goto error;
   }

   free(line_cpy);
   return 0;

error:
   free(line_cpy);

   return 1;
}

static int
prometheus_add_value(struct deque** values, const char* const value)
{
   bool new_val = false;
   struct deque *vals = NULL;
   struct prometheus_value* val = NULL;

   if (values == NULL)
   {
      goto error;
   }

   vals = *values;

   val = malloc(sizeof(*val));
   if (val == NULL)
   {
      goto error;
   }

   if (vals == NULL)
   {
      new_val = true;
      pgexporter_deque_create(true, &vals);

      if (vals == NULL)
      {
         goto error;
      }
   }

   val->timestamp = time(NULL); // Attach current timestamp.
   val->value = strdup(value);
   if (val->value == NULL)
   {
      if (new_val) {
         pgexporter_deque_destroy(vals);
      }
      goto error;
   }

   pgexporter_deque_add(*values, NULL, (uintptr_t) val, ValueRef);

   return 0;

error:

   free(val);

   return 1;
}

static int
prometheus_add_attribute(struct deque** attributes, const char* const key,
                         const char* const value)
{
   bool new_attr = false;
   struct deque *attrs = NULL;
   struct prometheus_attribute* attr = NULL;

   if (attrs == NULL)
   {
      goto error;
   }

   attrs = *attributes;

   attr = malloc(sizeof(*attr));
   if (attr == NULL)
   {
      goto error;
   }

   if (attrs == NULL)
   {
      new_attr = true;
      pgexporter_deque_create(true, &attrs);

      if (attrs == NULL)
      {
         goto error;
      }
   }

   attr->key = strdup(key);
   if (attr->key == NULL)
   {
      goto error;
   }

   attr->value = strdup(value);
   if (attr->value == NULL)
   {
      goto error;
   }

   pgexporter_deque_add(attrs, NULL, (uintptr_t) attr, ValueRef);

   return 0;

error:
   free(attr->key);
   free(attr->value);

   if (new_attr)
   {
      pgexporter_deque_destroy(attrs);
   }

   free(attr);

   return 1;
}

static int
prometheus_add_definition(struct prometheus_metric* metric, struct deque** attr, struct deque** val)
{
   struct deque *attrs = NULL;
   struct deque *vals = NULL;
   struct prometheus_attributes* def = NULL;

   if (attr == NULL || *attr == NULL || val == NULL || *val == NULL || metric == NULL)
   {
      goto errout;
   }

   attrs = *attr;
   vals = *val;

   def = malloc(sizeof(*def));
   if (def == NULL)
   {
      goto errout;
   }

   if (metric->definitions == NULL)
   {
      pgexporter_deque_create(true, &metric->definitions);
   }

   def->attributes = attrs;
   def->values = vals;

   *attr = NULL;
   *val = NULL;

   if (pgexporter_deque_add(metric->definitions, NULL, (uintptr_t) def, ValueRef))
   {
      goto errout_with_def;
   }

   return 0;

errout_with_def:
   pgexporter_deque_destroy(def->attributes);
   def->attributes = NULL;

   pgexporter_deque_destroy(def->values);
   def->values = NULL;

   free(def);
   def = NULL;

errout:
   return 1;
}

static int
prometheus_add_line(struct prometheus_metric* metric, char* line, const char* endpoint)
{
   struct deque* attrs = NULL;
   struct deque* vals = NULL;

   // attr and val are allocated here.
   if (parse_metric_line(metric, &attrs, &vals, line, endpoint))
   {
      goto error;
   }

   // attr and val have their ownership transferred.
   if (prometheus_add_definition(metric, &attrs, &vals))
   {
      goto error;
   }

   return 0;

error:
   pgexporter_deque_destroy(attrs);
   pgexporter_deque_destroy(vals);

   return 1;
}

static int
prometheus_parse_bridge_body(const char* endpoint, char* body, struct prometheus_bridge* bridge)
{
   char* line = NULL;
   char* saveptr = NULL;
   char name[MISC_LENGTH] = {0};
   char help[MISC_LENGTH] = {0};
   char type[MISC_LENGTH] = {0};
   struct prometheus_metric* metric = NULL;

   line = strtok_r(body, "\n", &saveptr); /* We ideally should not care if body is modified. */

   while (line != NULL)
   {
      if ((!strcmp(line, "") || !strcmp(line, "\r")) && metric->definitions->size > 0) /* Basically empty strings, empty lines, or empty Windows lines. */
      {
         /* Previous metric is over. */

         pgexporter_art_insert(bridge->metrics, (unsigned char*) metric->name, strlen(metric->name), (uintptr_t) metric, ValueRef);

         metric = NULL;
         continue;
      }
      else if (line[0] == '#')
      {
         if (!strncmp(&line[1], "HELP", 4))
         {
            sscanf(line, "#HELP %127s %127[^\n]", name, help);

            prometheus_metric_find_create(bridge, name, &metric);

            prometheus_metric_set_name(metric, name);
            prometheus_metric_set_help(metric, strdup(help));

         }
         else if (!strncmp(&line[1], "TYPE", 4))
         {
            sscanf(line, "#TYPE %127s %127[^\n]", name, type);
            prometheus_metric_set_type(metric, strdup(type));
            // assert(!strcmp(metric->name, name));
         }
         else
         {
            // TODO: Destroy all of `metric` during this as well.
            goto error;
         }
      }
      else
      {
         prometheus_add_line(metric, line, endpoint);
      }

      line = strtok_r(NULL, "\n", &saveptr);
   }

   return 0;

error:
   pgexporter_art_destroy(bridge->metrics);
   bridge->metrics = NULL;

   return 1;
}

static char*
endpoint_to_url(struct endpoint* e)
{
   char* url = NULL;

   url = pgexporter_append(url, "http://");
   url = pgexporter_append(url, e->host);
   url = pgexporter_append_char(url, ':');
   url = pgexporter_append_int(url, e->port);
   url = pgexporter_append(url, "/metrics");

   return url;
}
