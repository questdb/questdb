#ifndef NODE_LIBPQ_CONNECTION
#define NODE_LIBPQ_CONNECTION

#include <nan.h>
#include <libpq-fe.h>

class Connection : public Nan::ObjectWrap {
  public:
    static NAN_METHOD(Create);
    static NAN_METHOD(ConnectSync);
    static NAN_METHOD(Connect);
    static NAN_METHOD(ServerVersion);
    static NAN_METHOD(Socket);
    static NAN_METHOD(GetLastErrorMessage);
    static NAN_METHOD(Finish);
    static NAN_METHOD(Exec);
    static NAN_METHOD(ExecParams);
    static NAN_METHOD(Prepare);
    static NAN_METHOD(ExecPrepared);
    static NAN_METHOD(Clear);
    static NAN_METHOD(Ntuples);
    static NAN_METHOD(Nfields);
    static NAN_METHOD(Fname);
    static NAN_METHOD(Ftype);
    static NAN_METHOD(Getvalue);
    static NAN_METHOD(Getisnull);
    static NAN_METHOD(CmdStatus);
    static NAN_METHOD(CmdTuples);
    static NAN_METHOD(ResultStatus);
    static NAN_METHOD(ResultErrorMessage);
    static NAN_METHOD(ResultErrorFields);
    static NAN_METHOD(SendQuery);
    static NAN_METHOD(SendQueryParams);
    static NAN_METHOD(SendPrepare);
    static NAN_METHOD(SendQueryPrepared);
    static NAN_METHOD(GetResult);
    static NAN_METHOD(ConsumeInput);
    static NAN_METHOD(IsBusy);
    static NAN_METHOD(StartRead);
    static NAN_METHOD(StopRead);
    static NAN_METHOD(StartWrite);
    static NAN_METHOD(SetNonBlocking);
    static NAN_METHOD(IsNonBlocking);
    static NAN_METHOD(Flush);
#ifdef ESCAPE_SUPPORTED
    static NAN_METHOD(EscapeLiteral);
    static NAN_METHOD(EscapeIdentifier);
#endif
    static NAN_METHOD(Notifies);
    static NAN_METHOD(PutCopyData);
    static NAN_METHOD(PutCopyEnd);
    static NAN_METHOD(GetCopyData);
    static NAN_METHOD(Cancel);

    bool ConnectDB(const char* paramString);
    char* ErrorMessage();
    PGconn* pq;

  private:
    PGresult* lastResult;
    uv_poll_t read_watcher;
    uv_poll_t write_watcher;
    bool is_reffed;
    bool is_reading;

    Connection();

    static void on_io_readable(uv_poll_t* handle, int status, int revents);
    static void on_io_writable(uv_poll_t* handle, int status, int revents);
    void ReadStart();
    void ReadStop();
    void WriteStart();
    void WriteStop();
    void ClearLastResult();
    void SetLastResult(PGresult* result);
    static char* NewCString(v8::Local<v8::Value> val);
    static char** NewCStringArray(v8::Local<v8::Array> jsParams);
    static void DeleteCStringArray(char** array, int length);
    void Emit(const char* message);
};

#endif
