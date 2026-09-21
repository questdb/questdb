pushd ../thrift/compiler/cpp/
make
popd

curl https://raw.githubusercontent.com/apache/parquet-format/43c891a4494f85e2fe0e56f4ef408bcc60e8da48/src/main/thrift/parquet.thrift > parquet.thrift

../thrift/compiler/cpp/bin/thrift --gen rs parquet.thrift
mv parquet.rs src/parquet_format.rs
