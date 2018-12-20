'use strict';

let AWS = require('aws-sdk');
let s3 = new AWS.S3();
let https = require('https');
let md5 = require('md5');

let user_password;
let userId;

function post_to_api (endpoint_path, json, post_callback, callbackArgsOverride = undefined) {
  let post_options = {
    host: "api-dev.camera-trap.tw",
    port: '443',
    path: endpoint_path,
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': 'Basic ' + Buffer.from(user_password).toString('base64'),
      'camera-trap-user-id': userId
    }
  };

  let post_req = https.request(post_options, function(res) {
    res.setEncoding('utf8');
    let fullRes = "";
    res.on('data', function (data) {
      // console.log('Response: ' + _res);
      fullRes += data;
    });

    res.on('end', function(){
      
      let result;
      try {
        result = JSON.parse(fullRes);
      }
      catch (e) {
        console.log(e);
        console.log(fullRes);
      }
      
      if (result.error) {
        if (post_callback.onError) {
          post_callback.onError(result.error);
        }
      } else {
        if (callbackArgsOverride !== undefined) {
          post_callback.onSuccess(callbackArgsOverride);
        } else {
          post_callback.onSuccess(result);
        }
      }
      // context.succeed();
    })


    res.on('error', function (e) {
      console.log("Got error: " + e.message);
      context.done(null, 'FAILURE');
    });
  });

  // post the data
  post_req.write(JSON.stringify(json));
  post_req.end();
}

exports.handler = (event, context, callback) => {

  let bucket = event.Records[0].s3.bucket.name;

  let file_key = "credentials/aws-lamda-api-user.txt";
  let params = { Bucket: bucket, Key: file_key };

  // get user password from s3
  s3.getObject(params, function(err, data){

    user_password = data.Body.toString();
    let file_key = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, ' '));
    let uploadSessionId = file_key.split("/")[1];
    console.log('-----------' + uploadSessionId);
    let params = { Bucket: bucket, Key: file_key };

    // get obj tags
    s3.getObjectTagging(params, function(err, tags) {
      if (err) console.log(err, err.stack); // an error occurred
      else     console.log(tags);

      let tag_data = {};
      tags.TagSet.forEach(function(d){
        tag_data[d.Key] = d.Value;
      });

      // userId = file_key.split("/")[2];
      userId = tag_data.userId;

      if (!tag_data.subSite) tag_data.subSite = 'NULL';

      // get mma and mmm json
      s3.getObject(params, function(err, data){
        if (err) {
          console.log(err);
        }
        else {

          // console.log(params);

          // TODO: MUST REWRITE these https post requests
          let fullCameraLocation = tag_data.projectId + "/" + tag_data.site + "/" + tag_data.subSite + "/" + tag_data.cameraLocation;
          let fullCameraLocationMd5 = md5(fullCameraLocation);
          let lock_post_data = [
            {
              fullCameraLocationMd5: fullCameraLocationMd5,
              projectId: tag_data.projectId,
              "locked": true,
              "locked_by": tag_data.userId,
              "locked_on": Date.now() / 1000
            }
          ];

          // console.log(lock_post_data);
          // post the data
          post_to_api ("/camera-location/data-lock/bulk-replace", lock_post_data, { onSuccess: postJson, onError: jsonUploadError });


          function postJson () {
            console.log('-----------postJson');
            let json_string, json;
            try {
              json_string = data.Body.toString();
              json = JSON.parse(json_string);
            }
            catch (e) {
              //console.log(json);
              console.log(e);
              console.log(json_string);
            }

            // An object of options to indicate where to post to
            let force_import = (event.force_import === undefined) ? true : event.force_import;

            if (force_import || !json.hold) {
              post_to_api(json.endpoint, json.post, { onSuccess: updateProjectDataSpan, onError: jsonUploadError }, json.post);
            }
          } // end of func postJson
          
          function updateProjectDataSpan (mmxJson) {

            console.log('-----------updateProjectDataSpan');

            let maxTimestamp = -Infinity;
            let minTimestamp = Infinity;
            let maxDateTime = '';
            let minDateTime = '';
            
            mmxJson.forEach(jo => {
              let dateTimeCorrectedTimestamp = jo.$set.date_time_corrected_timestamp || jo.$setOnInsert.date_time_corrected_timestamp;
              if (dateTimeCorrectedTimestamp > maxTimestamp) {
                maxTimestamp = dateTimeCorrectedTimestamp;
                maxDateTime = jo.$set.corrected_date_time || jo.$setOnInsert.corrected_date_time;
              }
              if (dateTimeCorrectedTimestamp < minTimestamp) {
                minTimestamp = dateTimeCorrectedTimestamp;
                minDateTime = jo.$set.corrected_date_time || jo.$setOnInsert.corrected_date_time;
              }
            });

            let modified = Date.now() / 1000;
            post_to_api(
              '/upload-session/bulk-update',[{
                _id: uploadSessionId,
                projectId: tag_data.projectId,
                $set: {
                  status: "SUCCESS",
                  modified: modified,
                  earliestDataDate: minDateTime,
                  latestDataDate: maxDateTime
                },
                $setOnInsert: {
                  _id: uploadSessionId,
                  upload_session_id: uploadSessionId,
                  fullCameraLocationMd5: fullCameraLocationMd5,
                  projectTitle: tag_data.projectTitle,
                  projectId: tag_data.projectId,
                  by: tag_data.userId,
                },
                $upsert: true
              }],
              {onSuccess: console.log}
            );            


            https.get(`https://api-dev.camera-trap.tw/project/${tag_data.projectId}`, (resp) => {
              let data = '';
            
              // A chunk of data has been recieved.
              resp.on('data', (chunk) => {
                data += chunk;
              });
            
              // The whole response has been received. Print out the result.
              resp.on('end', () => {
                let prj; 
                try {
                  prj = JSON.parse(data);
                }
                catch (e) {
                  console.log(e);
                  console.log(data);
                }
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
    
                post_to_api('/project/bulk-update', update_project_data_span, { onSuccess: context.succeed }, null);

              });
            }).on("error", (err) => {
              console.log("Error: " + err.message);
              context.done(null, 'FAILURE');
            });

            unlockLocation();

          } // end of func updateProjectDataSpan

          function jsonUploadError (uploadErr) {
            console.log('-----------jsonUploadError');
            let modified = Date.now() / 1000;
            let uploadError = [{
              _id: uploadSessionId,
              projectId: tag_data.projectId,
              $set: {
                status: "ERROR",
                modified: modified
              },
              $push: {
                messages: {
                  problematic_ids: [],
                  key: file_key,
                  errors: [uploadErr.message],
                  modified: modified,
                }
              },
              $setOnInsert: {
                _id: uploadSessionId,
                upload_session_id: uploadSessionId,
                fullCameraLocationMd5: fullCameraLocationMd5,
                projectTitle: tag_data.projectTitle,
                projectId: tag_data.projectId,
                by: tag_data.userId,
              },
              $upsert: true
            }];
            console.log(uploadError);
            post_to_api(
              '/upload-session/bulk-update',
              uploadError,
              { onSuccess: console.log, onError: console.log }
            );

            unlockLocation();
          }

          function unlockLocation () {
            let unlock_post_data = [
              {
                fullCameraLocationMd5: fullCameraLocationMd5,
                projectId: tag_data.projectId,
                "locked": false,
                "locked_by": tag_data.userId,
                "locked_on": Date.now() / 1000
              }
            ];
  
            // console.log(unlock_post_data);
            // post the data
            post_to_api ("/camera-location/data-lock/bulk-replace", unlock_post_data, { onSuccess: console.log, onError: console.log });
          }

        }
      }); // get object
    }); // get object tagging
  });
}
