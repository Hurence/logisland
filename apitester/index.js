const express = require('express');
const app = express();
const port = process.env.PORT || 3000;
const fs = require('fs');
const promisify = require('util').promisify;
const filename = './logisland.json';
const bodyParser = require('body-parser');

app.use(bodyParser.json());
app.listen(port);

const routes = {

  getDataflows: async function(req, res) {
    console.log("Updating dataflow for logisland job", req.params.jobId);
    let content = JSON.parse(await promisify(fs.readFile)(filename));
    let fileDate = (await promisify(fs.stat)(filename)).mtime;
    res.append('Last-Modified', fileDate.toUTCString()).json(content);
  },

  notifyActiveDataflows: function(req, res) {
    console.log("Logisland active dataflows for job", req.params.jobId);
    console.log(JSON.stringify(req.body));
    res.sendStatus(200);
  }
};

app.route('/dataflows/:jobId')
  .get(routes.getDataflows)
  .post(routes.notifyActiveDataflows);

console.log('Logisland RESTful API server started on: ' + port);
