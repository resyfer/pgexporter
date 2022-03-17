/*
 * Copyright (C) 2021 Red Hat
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

#ifndef PGEXPORTER_LOGGING_H
#define PGEXPORTER_LOGGING_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdlib.h>

#define PGEXPORTER_LOGGING_TYPE_CONSOLE 0
#define PGEXPORTER_LOGGING_TYPE_FILE    1
#define PGEXPORTER_LOGGING_TYPE_SYSLOG  2

#define PGEXPORTER_LOGGING_LEVEL_DEBUG5  1
#define PGEXPORTER_LOGGING_LEVEL_DEBUG4  1
#define PGEXPORTER_LOGGING_LEVEL_DEBUG3  1
#define PGEXPORTER_LOGGING_LEVEL_DEBUG2  1
#define PGEXPORTER_LOGGING_LEVEL_DEBUG1  2
#define PGEXPORTER_LOGGING_LEVEL_INFO    3
#define PGEXPORTER_LOGGING_LEVEL_WARN    4
#define PGEXPORTER_LOGGING_LEVEL_ERROR   5
#define PGEXPORTER_LOGGING_LEVEL_FATAL   6

#define pgexporter_log_trace(...) pgexporter_log_line(PGEXPORTER_LOGGING_LEVEL_DEBUG5, __FILE__, __LINE__, __VA_ARGS__)
#define pgexporter_log_debug(...) pgexporter_log_line(PGEXPORTER_LOGGING_LEVEL_DEBUG1, __FILE__, __LINE__, __VA_ARGS__)
#define pgexporter_log_info(...)  pgexporter_log_line(PGEXPORTER_LOGGING_LEVEL_INFO,  __FILE__, __LINE__,  __VA_ARGS__)
#define pgexporter_log_warn(...)  pgexporter_log_line(PGEXPORTER_LOGGING_LEVEL_WARN,  __FILE__, __LINE__, __VA_ARGS__)
#define pgexporter_log_error(...) pgexporter_log_line(PGEXPORTER_LOGGING_LEVEL_ERROR, __FILE__, __LINE__, __VA_ARGS__)
#define pgexporter_log_fatal(...) pgexporter_log_line(PGEXPORTER_LOGGING_LEVEL_FATAL, __FILE__, __LINE__, __VA_ARGS__)

/**
 * Start the logging system
 * @return 0 upon success, otherwise 1
 */
int
pgexporter_start_logging(void);

/**
 * Stop the logging system
 * @return 0 upon success, otherwise 1
 */
int
pgexporter_stop_logging(void);

void
pgexporter_log_line(int level, char* file, int line, char* fmt, ...);

void
pgexporter_log_mem(void* data, size_t size);

#ifdef __cplusplus
}
#endif

#endif

