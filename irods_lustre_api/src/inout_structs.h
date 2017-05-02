typedef struct {
    int  _this;
    char _that [64];
} helloInp_t;

typedef struct {
    double _value;
} otherOut_t;

typedef struct {
    int  _this;
    char _that [64];
    otherOut_t _other;
} helloOut_t;

typedef struct {
    char *change_log_json;
} irodsLustreApiInp_t;
#define IrodsLustreApiInp_PI "str *change_log_json;"

typedef struct {
    int status;
} irodsLustreApiOut_t;
#define IrodsLustreApiOut_PI "int status;"


