[![Codacy Badge](https://api.codacy.com/project/badge/grade/83c6250bd9fc45a98c12c191af710754)](https://www.codacy.com/app/bluestreak/nfsdb)
[![CircleCI](https://circleci.com/gh/bluestreak01/questdb.svg?style=svg)](https://circleci.com/gh/bluestreak01/questdb)
[![Slack Status](https://serieux-saucisson-79115.herokuapp.com/badge.svg)](https://serieux-saucisson-79115.herokuapp.com/)

QuestDB is a relational database that natively supports time series data. It is specifically designed for the single purpose of ensuring consistently low response times from both data input and query interfaces, without sacrificing functionality.

## UI Screenshots


Drag-Drop bulk import
![Import Progress](https://cloud.githubusercontent.com/assets/7276403/16665958/70eecec8-447d-11e6-8e78-1437c9c15db5.png)


Automatic format recognition 
![Data Import Summary](https://cloud.githubusercontent.com/assets/7276403/16666673/ae88722c-4480-11e6-96d3-cd309475ca9d.png)


Query editor
![Query Editor](https://cloud.githubusercontent.com/assets/7276403/16667611/5339f3fa-4485-11e6-89d3-e2c92c440bd6.png "Query Editor")

## License

QuestDB is licensed under GNU Affero General Public License (AGPLv3).

## Documentation

Documentation is a work in progress: https://docs.questdb.org

## Releases

Multi-platform archive can be downloaded from our web site https://www.questdb.org. Embedded database is available on maven central at these coordinates.

```xml
<dependency>
    <groupId>org.questdb</groupId>
    <artifactId>questdb-core</artifactId>
    <version>1.0.4</version>
</dependency>
```

On MacOS run (via [Homebrew](https://brew.sh/)):
```
brew install questdb
```
