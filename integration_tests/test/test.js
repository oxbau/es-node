// javascript
const core = require('@actions/core');

var errMessage = "error message ++++++++++++++"

console.log("test:", "starting");

core.info('test started')

core.setFailed(`Action failed with error ${errMessage}`);

console.log("test:", "ending");

core.info('test ended');
