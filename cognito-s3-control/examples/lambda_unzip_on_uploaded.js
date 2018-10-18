'use strict';

let AWS = require('aws-sdk');
let s3 = new AWS.S3();
// let PassThrough = require('stream').PassThrough;
// let Writable = require('stream').Writable;
let unzip = require('unzip-stream');
let md5 = require('md5');
let streamBuffers = require('stream-buffers');
let ExifImage = require('exif').ExifImage;
let iconvLite = require('iconv-lite');
const sharp = require('sharp');

exports.handler = (event, context, callback) => {
  
  console.log(event.Records[0].s3.object);
  console.log(event.Records[0].s3);

  let bucket = event.Records[0].s3.bucket.name;

  let file_key = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, ' '));
  let file_key_name_part = file_key.split("/").pop();
  let params = { Bucket: bucket, Key: file_key };
  let upload_session_id = file_key.split("/")[2];

  s3.getObjectTagging(params, function(err, tags) {
    if (err) console.log(err, err.stack); // an error occurred
    else     console.log(tags);

    let tag_data = {};
    tags.TagSet.forEach(function(d){
      tag_data[d.Key] = d.Value;
    });

    function encodeQueryData (data) {
      let ret = [];
      for (let d in data) {
        if (data.hasOwnProperty(d)) {
          ret.push(encodeURIComponent(d) + '=' + encodeURIComponent(data[d]));
        }
      }
      return ret.join('&');
    }

    let tags_string = encodeQueryData(tag_data);

    if (!tag_data.sub_site) tag_data.sub_site = 'NULL';

    let parser = unzip.Parse({ decodeString: (buffer) => { return iconvLite.decode(buffer, 'utf8'); } });

    let upsert_querys = [];
    let relative_url_json = "camera-trap/json/" + upload_session_id + "/" + file_key_name_part + ".json";
    let unzip_end = false;
    let cnt_of_exif_extracting = 0;

    s3.getObject(params).createReadStream()
      .pipe(parser)
      .on('entry', function (entry) {
        let fileName = entry.path;
        // let type = entry.type; // 'Directory' or 'File'
        // let size = entry.size;
        
        if (!fileName.match(/\.jpg$|\.jpeg$/i)) {
          entry.autodrain();
        }
        else {
          // console.log(entry);
          // console.log("File: " + fileName + ", Type: " + type + ", Size: " + size);
          let fileWritableStreamBuffer = new streamBuffers.WritableStreamBuffer({
              initialSize: (100 * 1024),   // start at 100 kilobytes.
              incrementAmount: (100 * 1024) // grow by 10 kilobytes each time buffer overflows.
          });
    
          cnt_of_exif_extracting++;
          entry.pipe(fileWritableStreamBuffer).on('finish', function() {

            console.log("get head of " + fileName + ":");
            let file_size = fileWritableStreamBuffer.size();
            console.log(file_size / 1024 + "kb");

            // 理想狀況這時應該拿得到 EXIF
            let file_buf = fileWritableStreamBuffer.getContents();
            new ExifImage(file_buf, function (error, exifData) {
              if (error)
                console.log('Error: '+error.message);
              else {
                console.log(exifData); // Do something with your data!

                let dateTimeComponents = exifData.exif.DateTimeOriginal.match(/\d+/g);
                let dateTimeString = dateTimeComponents[0] + "-" + dateTimeComponents[1] + "-" + dateTimeComponents[2] + " " + dateTimeComponents[3] + ":" + dateTimeComponents[4] + ":" + dateTimeComponents[5];
                let timestamp = new Date(dateTimeString).getTime() / 1000;
                let baseFileName = fileName.split("/").pop();
                let baseFileNameParts = baseFileName.split(".");
                // if (baseFileNameParts.length > 1)
                // let extname = baseFileNameParts.pop();
                baseFileName = baseFileNameParts.join(".") + "_" + timestamp;

                console.log("Remain size: " + fileWritableStreamBuffer.size() / 1024 + "kb");

                let full_location = tag_data.project + "/" + tag_data.site + "/" + tag_data.sub_site + "/" + tag_data.location;
                let relocate_path = "camera-trap/images/orig/" + full_location;

                let relative_url = relocate_path + '/' + baseFileName + ".jpg";
                  
                let _id = md5(relative_url);
                let full_location_md5 = md5(full_location);

                let upsert_query = {
                  _id: _id,
                  project: tag_data.project,
                  full_location_md5: full_location_md5,
                  $setOnInsert: {
                    url: relative_url,
                    url_md5: _id,
                    date_time_original: exifData.exif.DateTimeOriginal,
                    date_time_original_timestamp: timestamp,
                    project: tag_data.project,
                    site: tag_data.site,
                    sub_site: tag_data.sub_site,
                    location: tag_data.location,
                    full_location_md5: full_location_md5,
                    timezone: "+8",
                    tokens:[{}]
                  },
                  $addToSet: {related_upload_sessions: upload_session_id},
                  $upsert: true
                };

                upsert_querys.push(upsert_query);

                // original file upload
                if (file_size)
                s3.upload({Bucket: bucket, Key: relative_url, Body: file_buf, ContentType: "image/jpeg", Tagging: tags_string}, {},
                  function(err, data) {
                    if (err) 
                      console.log('ERROR!');
                    else
                      console.log('OK');
                  });
                //*

              } // end of exif extraction
              cnt_of_exif_extracting--;
              if (cnt_of_exif_extracting == 0 && unzip_end) {
                let upsert_querys_string = JSON.stringify(upsert_querys, null, 2);
                console.log(relative_url_json);
                s3.upload({Bucket: bucket, Key: relative_url_json, Body: upsert_querys_string, ContentType: "application/json"}, {},
                  function(err, data) {
                    if (err) 
                      console.log('ERROR!');
                    else
                      console.log('OK');
                  });
                // console.log(JSON.stringify(upsert_querys, null, 2));
              }

            });

            // 暫時不處理縮圖
            // diff resolutions and quality settings
            if (0) // turn off testing
            for (let res_idx = 4; res_idx <= 8; res_idx++) {
              for (let quality = 60; quality <= 80; quality = quality + 10) {
                let width = 128 * res_idx;
                let height = 3 * width / 4;

                sharp(file_buf)
                  .withMetadata()
                  .resize(width, height)
                  .webp({quality: quality})
                  // .jpeg({quality: 80})
                  // .webp({ lossless: true })
                  .toBuffer()
                  .then(function(resized_data){
                    // 再用EXIF做為重新命名的依據
                    if (file_size)
                    s3.upload({Bucket: bucket, Key: 'public_images/' + width + 'q' + quality + '/' + fileName + ".webp", Body: resized_data, ContentType: "image/webp"}, {},
                      function(err, data) {
                        if (err) 
                          console.log('ERROR!');
                        else
                          console.log('OK');
                      });

                  });
                }
            }
          });
        }
      }) // end of on entry
      .on('finish', function() {
        console.log("***************** UNZIP FINISH *****************");
      })
      .on('end', function() {
        unzip_end = true;
        console.log("***************** UNZIP END *****************");
      })
      .on('done', function() {
        console.log("***************** UNZIP DONE *****************");
      })
      .on('close', function() {
        console.log("***************** UNZIP CLOSE *****************");
      }); 
  });

}


