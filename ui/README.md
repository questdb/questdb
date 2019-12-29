## Configure build

In IntelliJ Idea settings set JavaScript language level to 'ECMAScript 6'. Without it IDEA will incorrectly format gulpfile.

## Building

Download Node: https://nodejs.org/en/download/

Run these commands once to setup dependencies & build

- npm install
- npm install -g bower
- npm install -g gulp
- bower install
- gulp

Run this command to build source code after source code change:

- gulp
