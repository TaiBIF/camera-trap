'use strict';

let AWS = require('aws-sdk');
let s3 = new AWS.S3();
let md5 = require('md5');
let streamBuffers = require('stream-buffers');
let ExifImage = require('exif').ExifImage;
const sharp = require('sharp');

let species_field = "species";

exports.handler = (event, context, callback) => {

  console.log(event.Records[0].s3.object);
  console.log(event.Records[0].s3);

  let bucket = event.Records[0].s3.bucket.name;

  let file_key = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, ' '));
  let file_key_name_part = file_key.split("/").pop();
  let params = { Bucket: bucket, Key: file_key };
  let upload_session_id = file_key.split("/")[1];

  // let root_dir = "camera-trap/";
  let root_dir = "";


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

    if (!tag_data.subSite) tag_data.subSite = 'NULL';

    // let parser = unzip.Parse({ decodeString: (buffer) => { return iconvLite.decode(buffer, 'utf8'); } });

    let mma_upsert_querys = [];
    let mma_relative_url_json = root_dir + "json/" + upload_session_id + "/" + tag_data.userId + "/" + file_key_name_part + ".mma.json";

    let mmm_upsert_querys = [];
    let mmm_relative_url_json = root_dir + "json/" + upload_session_id + "/" + tag_data.userId + "/" + file_key_name_part + ".mmm.json";

    let fileWritableStreamBuffer = new streamBuffers.WritableStreamBuffer({
      initialSize: (100 * 1024),   // start at 100 kilobytes.
      incrementAmount: (100 * 1024) // grow by 100 kilobytes each time buffer overflows.
    });

    s3.getObject(params).createReadStream()
      .pipe(fileWritableStreamBuffer)
      .on('finish', function() {
        let fileName = file_key;

        let baseFileName = fileName.split("/").pop();
        let uploaded_baseFileName = baseFileName;
        let baseFileNameParts = baseFileName.split(".");
        // if (baseFileNameParts.length > 1)
        baseFileNameParts.pop();
        
        let fullCameraLocation = tag_data.projectId + "/" + tag_data.site + "/" + tag_data.subSite + "/" + tag_data.cameraLocation;
        let relocate_path = root_dir + "images/orig/" + fullCameraLocation;
        let relocate_path_low_quality = root_dir + "images/_res_quality_/" + fullCameraLocation;

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

            // 太大了，暫時不存
            delete exifData.exif.MakerNote;

            let dateTimeComponents = exifData.exif.DateTimeOriginal.match(/\d+/g);
            let dateTimeString = dateTimeComponents[0] + "/" + dateTimeComponents[1] + "/" + dateTimeComponents[2] + " " + dateTimeComponents[3] + ":" + dateTimeComponents[4] + ":" + dateTimeComponents[5];

            let date_time_obj = new Date(dateTimeString + '+8');
            let timestamp = date_time_obj.getTime() / 1000;

            let year = date_time_obj.getFullYear();
            let month = date_time_obj.getMonth() + 1;
            let day = date_time_obj.getDate();
            let hour = date_time_obj.getHours();

            console.log("Remain size: " + fileWritableStreamBuffer.size() / 1024 + "kb");

            baseFileName = baseFileNameParts.join(".") + "_" + timestamp;
            let relative_url = relocate_path + '/' + baseFileName + ".jpg";
            let relative_url_lq = relocate_path_low_quality + '/' + baseFileName + ".webp";

            let _id = md5(relative_url);
            let fullCameraLocationMd5 = md5(fullCameraLocation);

            // original file upload
            if (file_size) {
              s3.upload({Bucket: bucket, Key: relative_url, Body: file_buf, ACL: 'public-read', ContentType: "image/jpeg", Tagging: tags_string}, {},
                function(err, data) {
                  if (err)
                    console.log('ERROR!');
                  else
                    console.log('OK');
                }
              );
            }

            // create compressed image
            let quality = 60;
            let res_idx = 4;
            let width = 128 * res_idx;
            let height = 3 * width / 4;
            let webpRelativePath = relative_url_lq.replace("_res_quality_", width + "q" + quality);

            let mma_upsert_query = {
              _id: _id,
              projectId: tag_data.projectId,
              fullCameraLocationMd5: fullCameraLocationMd5,
              $set: { // 只能由多媒體檔案中擷取出的資訊，放在 $set。目的是補充先上傳 CSV 再上傳 多媒體檔時欠缺的 metadata
                modifiedBy: tag_data.userId,
                type: "StillImage",
                date_time_original: exifData.exif.DateTimeOriginal,
                date_time_original_timestamp: timestamp, // 這個值可從 CSV 中的拍照時間還原。或在相機設定錯誤時覆蓋掉 metadata
                low_quality_url: webpRelativePath,
                imageUrlPrefix: 'https://s3-ap-northeast-1.amazonaws.com/camera-trap/'
              },
              $setOnInsert: {
                url: relative_url,
                url_md5: _id,
                date_time_corrected_timestamp: timestamp,
                corrected_date_time: dateTimeString,
                projectId: tag_data.projectId,
                projectTitle: tag_data.projectTitle,
                site: tag_data.site,
                subSite: tag_data.subSite,
                cameraLocation: tag_data.cameraLocation,
                fullCameraLocationMd5: fullCameraLocationMd5,
                uploaded_file_name: uploaded_baseFileName,
                timezone: "+8",
                year: year,
                month: month,
                day: day,
                hour: hour,
                tokens:[{
                  data :[{
                    key: species_field,
                    label: "物種",
                    value: "尚未辨識"
                  }],
                  species_shortcut: '尚未辨識'
                }]
              },
              $addToSet: {related_upload_sessions: upload_session_id},
              $upsert: true
            };
            mma_upsert_querys.push(mma_upsert_query);

            let mmm_upsert_query = {
              _id: _id,
              projectId: tag_data.projectId,
              fullCameraLocationMd5: fullCameraLocationMd5,
              $set: {
                modifiedBy: tag_data.userId,
                type: "StillImage",
                date_time_original_timestamp: timestamp,
                date_time_original: exifData.exif.DateTimeOriginal,
                device_metadata: exifData.image,
                make: exifData.image.Make,
                model: exifData.image.Model,
                modify_date: exifData.image.ModifyDate,
                exif: exifData.exif,
                low_quality_url: webpRelativePath,
                imageUrlPrefix: 'https://s3-ap-northeast-1.amazonaws.com/camera-trap/'
              },
              $setOnInsert: {
                url: relative_url,
                url_md5: _id,
                date_time_corrected_timestamp: timestamp,
                corrected_date_time: dateTimeString,
                projectId: tag_data.projectId,
                projectTitle: tag_data.projectTitle,
                site: tag_data.site,
                subSite: tag_data.subSite,
                cameraLocation: tag_data.cameraLocation,
                fullCameraLocationMd5: fullCameraLocationMd5,
                uploaded_file_name: uploaded_baseFileName,
                timezone: "+8",
                year: year,
                month: month,
                day: day,
                hour: hour,
              },
              $upsert: true
            };
            mmm_upsert_querys.push(mmm_upsert_query);

            sharp(file_buf)
              .withMetadata()
              .resize(width, height)
              .webp({quality: quality})
              // .jpeg({quality: 80})
              // .webp({ lossless: true })
              .toBuffer()
              .then(function(resized_data){
                // 再用EXIF做為重新命名的依據
                console.log(relocate_path_low_quality);
                if (file_size)
                s3.upload({Bucket: bucket, Key: webpRelativePath, Body: resized_data, ACL: 'public-read', ContentType: "image/webp", Tagging: tags_string}, {},
                  function(err, data) {
                    if (err)
                      console.log('ERROR!');
                    else
                      console.log('OK');
                  }
                );
              });
            } // end of exif extraction

            let mma_op = {
              endpoint: "/media/annotation/bulk-update",
              post: mma_upsert_querys
            }
            let mma_upsert_querys_string = JSON.stringify(mma_op, null, 2);
            console.log(mma_relative_url_json);
            s3.upload({Bucket: bucket, Key: mma_relative_url_json, Body: mma_upsert_querys_string, ContentType: "application/json", Tagging: tags_string}, {},
              function(err, data) {
                if (err)
                  console.log('ERROR!');
                else
                  console.log('OK');
              }
            );
            // console.log(JSON.stringify(mma_upsert_querys, null, 2));
            // -------------
            let mmm_op = {
              endpoint:"/media/bulk-update",
              post: mmm_upsert_querys
            }
            let mmm_upsert_querys_string = JSON.stringify(mmm_op, null, 2);
            console.log(mmm_relative_url_json);
            s3.upload({Bucket: bucket, Key: mmm_relative_url_json, Body: mmm_upsert_querys_string, ContentType: "application/json", Tagging: tags_string}, {},
              function(err, data) {
                if (err)
                  console.log('ERROR!');
                else
                  console.log('OK');
              }
            );

        });
        // diff resolutions and quality settings
      });
  });

}


