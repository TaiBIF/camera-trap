'use strict';

let AWS = require('aws-sdk');
let s3 = new AWS.S3();
let https = require('https');
let md5 = require('md5');

exports.handler = (event, context, callback) => {

  let bucket = event.Records[0].s3.bucket.name;

  let file_key = "credentials/aws-lamda-api-user.txt";
  let params = { Bucket: bucket, Key: file_key };

  // get user password from s3
  s3.getObject(params, function(err, data){

    let user_password = data.Body.toString();
    let file_key = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, ' '));
    let userId = file_key.split("/")[2];
    let params = { Bucket: bucket, Key: file_key };

    // get obj tags
    s3.getObjectTagging(params, function(err, tags) {
      if (err) console.log(err, err.stack); // an error occurred
      else     console.log(tags);

      let tag_data = {};
      tags.TagSet.forEach(function(d){
        tag_data[d.Key] = d.Value;
      });

      if (!tag_data.subSite) tag_data.subSite = 'NULL';

      // get mma and mmm json
      s3.getObject(params, function(err, data){
        if (err) {
          console.log(err);
        }
        else {

          console.log(params);
          let base64UserPasswd = Buffer.from(user_password).toString('base64');

          // TODO: MUST REWRITE these https post requests

          // data lock api
          let lock_post_options = {
            host: "api-dev.camera-trap.tw",
            port: '443',
            path: "/camera-location/data-lock/bulk-replace",
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'Authorization': 'Basic ' + base64UserPasswd,
              'camera-trap-user-id': userId
            }
          };

          // Set up the lock data request
          let lock_post_req = https.request(lock_post_options, function(res) {
            res.setEncoding('utf8');
            res.on('data', function (chunk) {
              console.log('Response: ' + chunk);
              postJson();

            });
            res.on('error', function (e) {
              console.log("Got error: " + e.message);
              context.done(null, 'FAILURE');
            });
          });

          let fullCameraLocation = tag_data.projectId + "/" + tag_data.site + "/" + tag_data.subSite + "/" + tag_data.cameraLocation;
          let fullCameraLocationMd5 = md5(fullCameraLocation);
          let lock_post_data = JSON.stringify([
            {
              fullCameraLocationMd5: fullCameraLocationMd5,
              projectId: tag_data.projectId,
              projectTitle: tag_data.projectTitle,
              "locked": true,
              "locked_by": tag_data.userId,
              "locked_on": Date.now() / 1000
            }
          ]);

          console.log(lock_post_data);

          // post the data
          lock_post_req.write(lock_post_data);
          lock_post_req.end();


          function postJson () {

            let json_string = data.Body.toString();
            let json = JSON.parse(json_string);
            //console.log(json);

            let post_data = JSON.stringify(json.post);
            //console.log(post_data);

            // An object of options to indicate where to post to
            let post_options = {
              host: "api-dev.camera-trap.tw",
              port: '443',
              path: json.endpoint,
              method: 'POST',
              headers: {
                'Content-Type': 'application/json',
                'Authorization': 'Basic ' + base64UserPasswd,
                'camera-trap-user-id': userId
              }
            };


            let force_import = (event.force_import === undefined) ? true : event.force_import;

            if (force_import || !json.hold) {
            // Set up the request
              let post_req = https.request(post_options, function(res) {
                res.setEncoding('utf8');
                res.on('data', function (chunk) {
                  console.log('Response: ' + chunk);
                  updateProjectDataSpan(json.post);
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
          } // end of func postJson

          function updateProjectDataSpan (mmxJson) {

            let maxTimestamp = -Infinity;
            let minTimestamp = Infinity;
            mmxJson.forEach(jo => {
              let dateTimeCorrectedTimestamp = jo.$set.date_time_corrected_timestamp || jo.$setOnInsert.date_time_corrected_timestamp;
              if (dateTimeCorrectedTimestamp > maxTimestamp) maxTimestamp = dateTimeCorrectedTimestamp;
              if (dateTimeCorrectedTimestamp < minTimestamp) minTimestamp = dateTimeCorrectedTimestamp;
            });

            https.get(`https://api-dev.camera-trap.tw/project/${tag_data.projectId}`, (resp) => {
              let data = '';
            
              // A chunk of data has been recieved.
              resp.on('data', (chunk) => {
                data += chunk;
              });
            
              // The whole response has been received. Print out the result.
              resp.on('end', () => {
                let prj = JSON.parse(data);
                let {earliestRecordTimestamp, latestRecordTimestamp} = prj;
                if (!earliestRecordTimestamp) earliestRecordTimestamp = Infinity;
                if (!latestRecordTimestamp) latestRecordTimestamp = -Infinity;
    
                if (maxTimestamp > latestRecordTimestamp) latestRecordTimestamp = maxTimestamp;
                if (minTimestamp < earliestRecordTimestamp) earliestRecordTimestamp = minTimestamp;
    
                let update_project_data_span = [{
                  _id: tag_data.projectId,
                  projectId: tag_data.projectId,
                  $set: {
                    earliestRecordTimestamp,
                    latestRecordTimestamp
                  }
                }];
    
                // An object of options to indicate where to post to
                let post_options = {
                  host: "api-dev.camera-trap.tw",
                  port: '443',
                  path: '/project/bulk-update',
                  method: 'POST',
                  headers: {
                    'Content-Type': 'application/json',
                    'Authorization': 'Basic ' + base64UserPasswd,
                    'camera-trap-user-id': userId
                  }
                };
    
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
                post_req.write(JSON.stringify(update_project_data_span));
                post_req.end();

              });
            
            }).on("error", (err) => {
              console.log("Error: " + err.message);
              context.done(null, 'FAILURE');
            });
          } // end of func updateProjectDataSpan
        }
      }); // get object
    }); // get object tagging
  });
}
