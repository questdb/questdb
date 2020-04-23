const rimraf = require("rimraf")
const wrench = require("wrench")

rimraf.sync("../core/src/main/resources/site/public")
wrench.copyDirSyncRecursive("dist", "../core/src/main/resources/site/public")
