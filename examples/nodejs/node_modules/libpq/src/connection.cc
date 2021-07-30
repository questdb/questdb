#include "addon.h"

Connection::Connection() : Nan::ObjectWrap() {
  TRACE("Connection::Constructor");
  pq = NULL;
  lastResult = NULL;
  read_watcher.data = this;
  write_watcher.data = this;
  is_reading = false;
  is_reffed = false;
}

NAN_METHOD(Connection::Create) {
  TRACE("Building new instance");
  Connection* conn = new Connection();
  conn->Wrap(info.This());

  info.GetReturnValue().Set(info.This());
}

NAN_METHOD(Connection::ConnectSync) {
  TRACE("Connection::ConnectSync::begin");

  Connection *self = Nan::ObjectWrap::Unwrap<Connection>(info.This());

  self->Ref();
  self->is_reffed = true;
  bool success = self->ConnectDB(*Nan::Utf8String(info[0]));

  info.GetReturnValue().Set(success);
}

NAN_METHOD(Connection::Connect) {
  TRACE("Connection::Connect");

  Connection* self = NODE_THIS();

  v8::Local<v8::Function> callback = info[1].As<v8::Function>();
  LOG("About to make callback");
  Nan::Callback* nanCallback = new Nan::Callback(callback);
  LOG("About to instantiate worker");
  ConnectAsyncWorker* worker = new ConnectAsyncWorker(info[0].As<v8::String>(), self, nanCallback);
  LOG("Instantiated worker, running it...");
  self->Ref();
  self->is_reffed = true;
  Nan::AsyncQueueWorker(worker);
}

NAN_METHOD(Connection::Socket) {
  TRACE("Connection::Socket");

  Connection *self = NODE_THIS();
  int fd = PQsocket(self->pq);
  TRACEF("Connection::Socket::fd: %d\n", fd);

  info.GetReturnValue().Set(fd);
}

NAN_METHOD(Connection::GetLastErrorMessage) {
  Connection *self = NODE_THIS();
  char* errorMessage = PQerrorMessage(self->pq);

  info.GetReturnValue().Set(Nan::New(errorMessage).ToLocalChecked());
}

NAN_METHOD(Connection::Finish) {
  TRACE("Connection::Finish::finish");

  Connection *self = NODE_THIS();

  self->ReadStop();
  self->ClearLastResult();
  PQfinish(self->pq);
  self->pq = NULL;
  if(self->is_reffed) {
    self->is_reffed = false;
    //self->Unref();
  }
}

NAN_METHOD(Connection::ServerVersion) {
  TRACE("Connection::ServerVersion");
  Connection* self = NODE_THIS();
  info.GetReturnValue().Set(PQserverVersion(self->pq));
}


NAN_METHOD(Connection::Exec) {
  Connection *self = NODE_THIS();
  Nan::Utf8String commandText(info[0]);

  TRACEF("Connection::Exec: %s\n", *commandText);
  PGresult* result = PQexec(self->pq, *commandText);

  self->SetLastResult(result);
}

NAN_METHOD(Connection::ExecParams) {
  Connection *self = NODE_THIS();

  Nan::Utf8String commandText(info[0]);
  TRACEF("Connection::Exec: %s\n", *commandText);

  v8::Local<v8::Array> jsParams = v8::Local<v8::Array>::Cast(info[1]);

  int numberOfParams = jsParams->Length();
  char **parameters = NewCStringArray(jsParams);

  PGresult* result = PQexecParams(
      self->pq,
      *commandText,
      numberOfParams,
      NULL, //const Oid* paramTypes[],
      parameters, //const char* const* paramValues[]
      NULL, //const int* paramLengths[]
      NULL, //const int* paramFormats[],
      0 //result format of text
      );

  DeleteCStringArray(parameters, numberOfParams);

  self->SetLastResult(result);
}

NAN_METHOD(Connection::Prepare) {
  Connection *self = NODE_THIS();

  Nan::Utf8String statementName(info[0]);
  Nan::Utf8String commandText(info[1]);
  int numberOfParams = Nan::To<int>(info[2]).FromJust();

  TRACEF("Connection::Prepare: %s\n", *statementName);

  PGresult* result = PQprepare(
      self->pq,
      *statementName,
      *commandText,
      numberOfParams,
      NULL //const Oid* paramTypes[]
      );

  self->SetLastResult(result);
}

NAN_METHOD(Connection::ExecPrepared) {
  Connection *self = NODE_THIS();

  Nan::Utf8String statementName(info[0]);

  TRACEF("Connection::ExecPrepared: %s\n", *statementName);

  v8::Local<v8::Array> jsParams = v8::Local<v8::Array>::Cast(info[1]);

  int numberOfParams = jsParams->Length();
  char** parameters = NewCStringArray(jsParams);

  PGresult* result = PQexecPrepared(
      self->pq,
      *statementName,
      numberOfParams,
      parameters, //const char* const* paramValues[]
      NULL, //const int* paramLengths[]
      NULL, //const int* paramFormats[],
      0 //result format of text
      );

  DeleteCStringArray(parameters, numberOfParams);

  self->SetLastResult(result);
}


NAN_METHOD(Connection::Clear) {
  TRACE("Connection::Clear");
  Connection *self = NODE_THIS();

  self->ClearLastResult();
}

NAN_METHOD(Connection::Ntuples) {
  TRACE("Connection::Ntuples");
  Connection *self = NODE_THIS();
  PGresult* res = self->lastResult;
  int numTuples = PQntuples(res);

  info.GetReturnValue().Set(numTuples);
}

NAN_METHOD(Connection::Nfields) {
  TRACE("Connection::Nfields");
  Connection *self = NODE_THIS();
  PGresult* res = self->lastResult;
  int numFields = PQnfields(res);

  info.GetReturnValue().Set(numFields);
}

NAN_METHOD(Connection::Fname) {
  TRACE("Connection::Fname");
  Connection *self = NODE_THIS();

  PGresult* res = self->lastResult;

  char* colName = PQfname(res, Nan::To<int32_t>(info[0]).FromJust());

  if(colName == NULL) {
    return info.GetReturnValue().SetNull();
  }

  info.GetReturnValue().Set(Nan::New<v8::String>(colName).ToLocalChecked());
}

NAN_METHOD(Connection::Ftype) {
  TRACE("Connection::Ftype");
  Connection *self = NODE_THIS();

  PGresult* res = self->lastResult;

  int colName = PQftype(res, Nan::To<int32_t>(info[0]).FromJust());

  info.GetReturnValue().Set(colName);
}

NAN_METHOD(Connection::Getvalue) {
  TRACE("Connection::Getvalue");
  Connection *self = NODE_THIS();

  PGresult* res = self->lastResult;

  int rowNumber = Nan::To<int32_t>(info[0]).FromJust();
  int colNumber = Nan::To<int32_t>(info[1]).FromJust();

  char* rowValue = PQgetvalue(res, rowNumber, colNumber);

  if(rowValue == NULL) {
    return info.GetReturnValue().SetNull();
  }

  info.GetReturnValue().Set(Nan::New(rowValue).ToLocalChecked());
}

NAN_METHOD(Connection::Getisnull) {
  TRACE("Connection::Getisnull");
  Connection *self = NODE_THIS();

  PGresult* res = self->lastResult;

  int rowNumber = Nan::To<int32_t>(info[0]).FromJust();
  int colNumber = Nan::To<int32_t>(info[1]).FromJust();

  int rowValue = PQgetisnull(res, rowNumber, colNumber);

  info.GetReturnValue().Set(rowValue == 1);
}

NAN_METHOD(Connection::CmdStatus) {
  TRACE("Connection::CmdStatus");
  Connection *self = NODE_THIS();

  PGresult* res = self->lastResult;
  char* status = PQcmdStatus(res);

  info.GetReturnValue().Set(Nan::New<v8::String>(status).ToLocalChecked());
}

NAN_METHOD(Connection::CmdTuples) {
  TRACE("Connection::CmdTuples");
  Connection *self = NODE_THIS();

  PGresult* res = self->lastResult;
  char* tuples = PQcmdTuples(res);

  info.GetReturnValue().Set(Nan::New<v8::String>(tuples).ToLocalChecked());
}

NAN_METHOD(Connection::ResultStatus) {
  TRACE("Connection::ResultStatus");
  Connection *self = NODE_THIS();

  PGresult* res = self->lastResult;

  char* status = PQresStatus(PQresultStatus(res));

  info.GetReturnValue().Set(Nan::New<v8::String>(status).ToLocalChecked());
}

NAN_METHOD(Connection::ResultErrorMessage) {
  TRACE("Connection::ResultErrorMessage");
  Connection *self = NODE_THIS();

  PGresult* res = self->lastResult;

  char* status = PQresultErrorMessage(res);

  info.GetReturnValue().Set(Nan::New<v8::String>(status).ToLocalChecked());
}

# define SET_E(key, name) \
  field = PQresultErrorField(self->lastResult, key); \
  if(field != NULL) { \
    Nan::Set(result, \
        Nan::New(name).ToLocalChecked(), Nan::New(field).ToLocalChecked()); \
  }

NAN_METHOD(Connection::ResultErrorFields) {
  Connection *self = NODE_THIS();

  if(self->lastResult == NULL) {
    return info.GetReturnValue().SetNull();
  }

  v8::Local<v8::Object> result = Nan::New<v8::Object>();
  char* field;
  SET_E(PG_DIAG_SEVERITY, "severity");
  SET_E(PG_DIAG_SQLSTATE, "sqlState");
  SET_E(PG_DIAG_MESSAGE_PRIMARY, "messagePrimary");
  SET_E(PG_DIAG_MESSAGE_DETAIL, "messageDetail");
  SET_E(PG_DIAG_MESSAGE_HINT, "messageHint");
  SET_E(PG_DIAG_STATEMENT_POSITION, "statementPosition");
  SET_E(PG_DIAG_INTERNAL_POSITION, "internalPosition");
  SET_E(PG_DIAG_INTERNAL_QUERY, "internalQuery");
  SET_E(PG_DIAG_CONTEXT, "context");
#ifdef MORE_ERROR_FIELDS_SUPPORTED
  SET_E(PG_DIAG_SCHEMA_NAME, "schemaName");
  SET_E(PG_DIAG_TABLE_NAME, "tableName");
  SET_E(PG_DIAG_COLUMN_NAME, "columnName");
  SET_E(PG_DIAG_DATATYPE_NAME, "dataTypeName");
  SET_E(PG_DIAG_CONSTRAINT_NAME, "constraintName");
#endif
  SET_E(PG_DIAG_SOURCE_FILE, "sourceFile");
  SET_E(PG_DIAG_SOURCE_LINE, "sourceLine");
  SET_E(PG_DIAG_SOURCE_FUNCTION, "sourceFunction");
  info.GetReturnValue().Set(result);
}

NAN_METHOD(Connection::SendQuery) {
  TRACE("Connection::SendQuery");

  Connection *self = NODE_THIS();
  Nan::Utf8String commandText(info[0]);

  TRACEF("Connection::SendQuery: %s\n", *commandText);
  int success = PQsendQuery(self->pq, *commandText);

  info.GetReturnValue().Set(success == 1);
}

NAN_METHOD(Connection::SendQueryParams) {
  TRACE("Connection::SendQueryParams");

  Connection *self = NODE_THIS();

  Nan::Utf8String commandText(info[0]);
  TRACEF("Connection::SendQueryParams: %s\n", *commandText);

  v8::Local<v8::Array> jsParams = v8::Local<v8::Array>::Cast(info[1]);

  int numberOfParams = jsParams->Length();
  char** parameters = NewCStringArray(jsParams);

  int success = PQsendQueryParams(
      self->pq,
      *commandText,
      numberOfParams,
      NULL, //const Oid* paramTypes[],
      parameters, //const char* const* paramValues[]
      NULL, //const int* paramLengths[]
      NULL, //const int* paramFormats[],
      0 //result format of text
      );

  DeleteCStringArray(parameters, numberOfParams);

  info.GetReturnValue().Set(success == 1);
}

NAN_METHOD(Connection::SendPrepare) {
  TRACE("Connection::SendPrepare");

  Connection *self = NODE_THIS();

  Nan::Utf8String statementName(info[0]);
  Nan::Utf8String commandText(info[1]);
  int numberOfParams = Nan::To<int>(info[2]).FromJust();

  TRACEF("Connection::SendPrepare: %s\n", *statementName);
  int success = PQsendPrepare(
      self->pq,
      *statementName,
      *commandText,
      numberOfParams,
      NULL //const Oid* paramTypes
      );

  info.GetReturnValue().Set(success == 1);
}

NAN_METHOD(Connection::SendQueryPrepared) {
  TRACE("Connection::SendQueryPrepared");

  Connection *self = NODE_THIS();

  Nan::Utf8String statementName(info[0]);
  TRACEF("Connection::SendQueryPrepared: %s\n", *statementName);

  v8::Local<v8::Array> jsParams = v8::Local<v8::Array>::Cast(info[1]);

  int numberOfParams = jsParams->Length();
  char** parameters = NewCStringArray(jsParams);

  int success = PQsendQueryPrepared(
      self->pq,
      *statementName,
      numberOfParams,
      parameters, //const char* const* paramValues[]
      NULL, //const int* paramLengths[]
      NULL, //const int* paramFormats[],
      0 //result format of text
      );

  DeleteCStringArray(parameters, numberOfParams);

  info.GetReturnValue().Set(success == 1);
}

NAN_METHOD(Connection::GetResult) {
  TRACE("Connection::GetResult");

  Connection *self = NODE_THIS();
  PGresult *result = PQgetResult(self->pq);

  if(result == NULL) {
    return info.GetReturnValue().Set(false);
  }

  self->SetLastResult(result);
  info.GetReturnValue().Set(true);
}

NAN_METHOD(Connection::ConsumeInput) {
  TRACE("Connection::ConsumeInput");

  Connection *self = NODE_THIS();

  int success = PQconsumeInput(self->pq);
  info.GetReturnValue().Set(success == 1);
}

NAN_METHOD(Connection::IsBusy) {
  TRACE("Connection::IsBusy");

  Connection *self = NODE_THIS();

  int isBusy = PQisBusy(self->pq);
  TRACEF("Connection::IsBusy: %d\n", isBusy);

  info.GetReturnValue().Set(isBusy == 1);
}

NAN_METHOD(Connection::StartRead) {
  TRACE("Connection::StartRead");

  Connection* self = NODE_THIS();

  self->ReadStart();
}

NAN_METHOD(Connection::StopRead) {
  TRACE("Connection::StopRead");

  Connection* self = NODE_THIS();

  self->ReadStop();
}

NAN_METHOD(Connection::StartWrite) {
  TRACE("Connection::StartWrite");

  Connection* self = NODE_THIS();

  self->WriteStart();
}

NAN_METHOD(Connection::SetNonBlocking) {
  TRACE("Connection::SetNonBlocking");

  Connection* self = NODE_THIS();

  int ok = PQsetnonblocking(self->pq, Nan::To<int>(info[0]).FromJust());

  info.GetReturnValue().Set(ok == 0);
}

NAN_METHOD(Connection::IsNonBlocking) {
  TRACE("Connection::IsNonBlocking");

  Connection* self = NODE_THIS();

  int status = PQisnonblocking(self->pq);

  info.GetReturnValue().Set(status == 1);
}

NAN_METHOD(Connection::Flush) {
  TRACE("Connection::Flush");

  Connection* self = NODE_THIS();

  int status = PQflush(self->pq);

  info.GetReturnValue().Set(status);
}

#ifdef ESCAPE_SUPPORTED
NAN_METHOD(Connection::EscapeLiteral) {
  TRACE("Connection::EscapeLiteral");

  Connection* self = NODE_THIS();

  Nan::Utf8String str(Nan::To<v8::String>(info[0]).ToLocalChecked());

  TRACEF("Connection::EscapeLiteral:input %s\n", *str);
  char* result = PQescapeLiteral(self->pq, *str, str.length());
  TRACEF("Connection::EscapeLiteral:output %s\n", result);

  if(result == NULL) {
    return info.GetReturnValue().SetNull();
  }

  info.GetReturnValue().Set(Nan::New(result).ToLocalChecked());
  PQfreemem(result);
}

NAN_METHOD(Connection::EscapeIdentifier) {
  TRACE("Connection::EscapeIdentifier");

  Connection* self = NODE_THIS();

  Nan::Utf8String str(Nan::To<v8::String>(info[0]).ToLocalChecked());

  TRACEF("Connection::EscapeIdentifier:input %s\n", *str);
  char* result = PQescapeIdentifier(self->pq, *str, str.length());
  TRACEF("Connection::EscapeIdentifier:output %s\n", result);

  if(result == NULL) {
    return info.GetReturnValue().SetNull();
  }

  info.GetReturnValue().Set(Nan::New(result).ToLocalChecked());
  PQfreemem(result);
}
#endif

NAN_METHOD(Connection::Notifies) {
  LOG("Connection::Notifies");

  Connection* self = NODE_THIS();

  PGnotify* msg = PQnotifies(self->pq);

  if(msg == NULL) {
    LOG("No notification");
    return;
  }

  v8::Local<v8::Object> result = Nan::New<v8::Object>();
  Nan::Set(result, Nan::New("relname").ToLocalChecked(), Nan::New(msg->relname).ToLocalChecked());
  Nan::Set(result, Nan::New("extra").ToLocalChecked(), Nan::New(msg->extra).ToLocalChecked());
  Nan::Set(result, Nan::New("be_pid").ToLocalChecked(), Nan::New(msg->be_pid));

  PQfreemem(msg);

  info.GetReturnValue().Set(result);
};

NAN_METHOD(Connection::PutCopyData) {
  LOG("Connection::PutCopyData");

  Connection* self = NODE_THIS();

  v8::Local<v8::Object> buffer = info[0].As<v8::Object>();

  char* data = node::Buffer::Data(buffer);
  int length = node::Buffer::Length(buffer);

  int result = PQputCopyData(self->pq, data, length);

  info.GetReturnValue().Set(result);
}

NAN_METHOD(Connection::PutCopyEnd) {
  LOG("Connection::PutCopyEnd");

  Connection* self = NODE_THIS();

  //optional error message

  bool sendErrorMessage = info.Length() > 0;
  int result;
  if(sendErrorMessage) {
    Nan::Utf8String msg(info[0]);
    TRACEF("Connection::PutCopyEnd:%s\n", *msg);
    result = PQputCopyEnd(self->pq, *msg);
  } else {
    result = PQputCopyEnd(self->pq, NULL);
  }

  info.GetReturnValue().Set(result);
}

static void FreeBuffer(char *buffer, void *) {
  PQfreemem(buffer);
}

NAN_METHOD(Connection::GetCopyData) {
  LOG("Connection::GetCopyData");

  Connection* self = NODE_THIS();

  char* buffer = NULL;
  int async = info[0]->IsTrue() ? 1 : 0;

  TRACEF("Connection::GetCopyData:async %d\n", async);

  int length = PQgetCopyData(self->pq, &buffer, async);

  //some sort of failure or not-ready condition
  if(length < 1) {
    return info.GetReturnValue().Set(length);
  }

  info.GetReturnValue().Set(Nan::NewBuffer(buffer, length, FreeBuffer, NULL).ToLocalChecked());
}

NAN_METHOD(Connection::Cancel) {
  LOG("Connection::Cancel");

  Connection* self = NODE_THIS();

  PGcancel *cancelStuct = PQgetCancel(self->pq);

  if(cancelStuct == NULL) {
    info.GetReturnValue().Set(Nan::Error("Unable to allocate cancel struct"));
    return;
  }

  char* errBuff = new char[255];

  LOG("PQcancel");
  int result = PQcancel(cancelStuct, errBuff, 255);

  LOG("PQfreeCancel");
  PQfreeCancel(cancelStuct);

  if(result == 1) {
    delete[] errBuff;
    return info.GetReturnValue().Set(true);
  }

  info.GetReturnValue().Set(Nan::New(errBuff).ToLocalChecked());
  delete[] errBuff;
}

bool Connection::ConnectDB(const char* paramString) {
  TRACEF("Connection::ConnectDB:Connection parameters: %s\n", paramString);
  this->pq = PQconnectdb(paramString);

  ConnStatusType status = PQstatus(this->pq);

  if(status != CONNECTION_OK) {
    return false;
  }

  int fd = PQsocket(this->pq);
  uv_poll_init_socket(uv_default_loop(), &(this->read_watcher), fd);
  uv_poll_init_socket(uv_default_loop(), &(this->write_watcher), fd);

  TRACE("Connection::ConnectSync::Success");
  return true;
}

char * Connection::ErrorMessage() {
  return PQerrorMessage(this->pq);
}

void Connection::on_io_readable(uv_poll_t* handle, int status, int revents) {
  LOG("Connection::on_io_readable");
  TRACEF("Connection::on_io_readable:status %d\n", status);
  TRACEF("Connection::on_io_readable:revents %d\n", revents);
  if(revents & UV_READABLE) {
    LOG("Connection::on_io_readable UV_READABLE");
    Connection* self = (Connection*) handle->data;
    LOG("Got connection pointer");
    self->Emit("readable");
  }
}

void Connection::on_io_writable(uv_poll_t* handle, int status, int revents) {
  LOG("Connection::on_io_writable");
  TRACEF("Connection::on_io_writable:status %d\n", status);
  TRACEF("Connection::on_io_writable:revents %d\n", revents);
  if(revents & UV_WRITABLE) {
    LOG("Connection::on_io_readable UV_WRITABLE");
    Connection* self = (Connection*) handle->data;
    self->WriteStop();
    self->Emit("writable");
  }
}

void Connection::ReadStart() {
  LOG("Connection::ReadStart:starting read watcher");
  is_reading = true;
  uv_poll_start(&read_watcher, UV_READABLE, on_io_readable);
  LOG("Connection::ReadStart:started read watcher");
}

void Connection::ReadStop() {
  LOG("Connection::ReadStop:stoping read watcher");
  if(!is_reading) return;
  is_reading = false;
  uv_poll_stop(&read_watcher);
  LOG("Connection::ReadStop:stopped read watcher");
}

void Connection::WriteStart() {
  LOG("Connection::WriteStart:starting write watcher");
  uv_poll_start(&write_watcher, UV_WRITABLE, on_io_writable);
  LOG("Connection::WriteStart:started write watcher");
}

void Connection::WriteStop() {
  LOG("Connection::WriteStop:stoping write watcher");
  uv_poll_stop(&write_watcher);
}


void Connection::ClearLastResult() {
  LOG("Connection::ClearLastResult");
  if(lastResult == NULL) return;
  PQclear(lastResult);
  lastResult = NULL;
}

void Connection::SetLastResult(PGresult* result) {
  LOG("Connection::SetLastResult");
  ClearLastResult();
  lastResult = result;
}

char* Connection::NewCString(v8::Local<v8::Value> val) {
  Nan::HandleScope scope;

  Nan::Utf8String str(val);
  char* buffer = new char[str.length() + 1];
  strcpy(buffer, *str);

  return buffer;
}

char** Connection::NewCStringArray(v8::Local<v8::Array> jsParams) {
  Nan::HandleScope scope;

  int numberOfParams = jsParams->Length();

  char** parameters = new char*[numberOfParams];

  for(int i = 0; i < numberOfParams; i++) {
    v8::Local<v8::Value> val = Nan::Get(jsParams, i).ToLocalChecked();
    if(val->IsNull()) {
      parameters[i] = NULL;
      continue;
    }
    //expect every other value to be a string...
    //make sure aggresive type checking is done
    //on the JavaScript side before calling
    parameters[i] = NewCString(val);
  }

  return parameters;
}

void Connection::DeleteCStringArray(char** array, int length) {
  for(int i = 0; i < length; i++) {
    delete [] array[i];
  }
  delete [] array;
}

void Connection::Emit(const char* message) {
  Nan::HandleScope scope;

  TRACE("ABOUT TO EMIT EVENT");
  v8::Local<v8::Object> jsInstance = handle();
  TRACE("GETTING 'emit' FUNCTION INSTANCE");
  v8::Local<v8::Value> emit_v = Nan::Get(jsInstance, Nan::New<v8::String>("emit").ToLocalChecked()).ToLocalChecked();
  assert(emit_v->IsFunction());
  v8::Local<v8::Function> emit_f = emit_v.As<v8::Function>();

  v8::Local<v8::String> eventName = Nan::New<v8::String>(message).ToLocalChecked();
  v8::Local<v8::Value> info[1] = { eventName };

  TRACE("CALLING EMIT");
  Nan::TryCatch tc;
  Nan::AsyncResource *async_emit_f = new Nan::AsyncResource("libpq:connection:emit");
  async_emit_f->runInAsyncScope(handle(), emit_f, 1, info);
  if(tc.HasCaught()) {
    Nan::FatalException(tc);
  }
}
