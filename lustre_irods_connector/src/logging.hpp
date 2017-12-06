#ifndef LUSTRE_CONNECTOR_DEBUG_LOGGING
#define LUSTRE_CONNECTOR_DEBUG_LOGGING

#define LOG_FATAL    (1)
#define LOG_ERR      (2)
#define LOG_WARN     (3)
#define LOG_INFO     (4)
#define LOG_DBG      (5)

#define LOG(level, ...) do {  \
                                if (level <= log_level) { \
                                                                    fprintf(dbgstream, __VA_ARGS__); \
                                                                    fflush(dbgstream); \
                                                                } \
                            } while (0)
extern FILE *dbgstream;
extern int  log_level;

#endif
