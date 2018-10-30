'use strict';

let AWS = require('aws-sdk');
let s3 = new AWS.S3();
let https = require('https');

exports.handler = (event, context, callback) => {

  let bucket = event.Records[0].s3.bucket.name;
  
  let file_key = "credentials/aws-lamda-api-user.txt";
  let params = { Bucket: bucket, Key: file_key };

  s3.getObject(params, function(err, data){

    let user_password = data.Body.toString();
    let file_key = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, ' '));
    let params = { Bucket: bucket, Key: file_key };

    s3.getObject(params, function(err, data){
      if (err) {
        console.log(err);
      }
      else {
        let json_string = data.Body.toString();
        let json = JSON.parse(json_string);
        //console.log(json);
        
        let post_data = JSON.stringify(json.post);
        //console.log(post_data);

        // An object of options to indicate where to post to
        let post_options = {
          host: "camera-trap.tw",
          port: '443',
          path: "/api" + json.endpoint,
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': 'Basic ' + Buffer.from(user_password).toString('base64')
          }
        };


        let force_import = (event.force_import === undefined) ? true : event.force_import;

        if (force_import || !json.hold) {
        // Set up the request
          let post_req = https.request(post_options, function(res) {
            res.setEncoding('utf8');
            res.on('data', function (chunk) {
              console.log('Response: ' + chunk);
              context.succeed();
            });
            res.on('error', function (e) {
              console.log("Got error: " + e.message);
              context.done(null, 'FAILURE');
            });
          });

          // post the data
          post_req.write(post_data);
          post_req.end();
        }
      }
    });
  
  });

  
  
}
