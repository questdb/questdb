# Feature Request: SHOW TABLES WITH SIZE

**Is your feature request related to a problem?**  
In PostgreSQL (or most SQL systems), getting a quick overview of all tables with their sizes takes a fairly verbose query involving system catalogs. For example:
```sql
SELECT relname AS table_name, pg_size_pretty(pg_total_relation_size(relid)) AS total_size
FROM pg_catalog.pg_statio_user_tables
ORDER BY pg_total_relation_size(relid) DESC;
```
This is not very user-friendly for quickly checking table sizes.

**Describe the solution you'd like** 
A new command:
```
SHOW TABLES WITH SIZE;
```
This would provide a simple overview of all tables and their sizes.

Optionally, this could be enhanced with:
```
SHOW TABLES WITH SIZE INCLUDING INDEXES;
```
to include index sizes in the output.

**Describe alternatives you've considered**  
The current alternative is to write a verbose query against system catalogs, which is less convenient and less readable.

**Full Name:**  
Prasun Bhattacharya

**Affiliation:**  
QuestDB

**Additional context**  
No response. 
