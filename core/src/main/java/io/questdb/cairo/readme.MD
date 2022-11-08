## Cairo

CAIRO is code name for next generation storage system for QuestDB. It targets goals and functionality that is not well
implemented in the original QuestDB storage.

Some of these goals are:

+ provide common data access model for both memory and memory mapped files
+ lightweight table open and close operations
+ clean separation of read and write concerns
+ support of DDL operations without need to close and re-open tables or copying data
+ excellent error reporting to enable easy operational support
+ exceptional resistance to data corruption
+ i18n support table names, column names and content
+ first class support for BLOBs

- first class support for data retention management
