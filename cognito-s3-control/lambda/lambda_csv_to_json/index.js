'use strict'

let AWS = require('aws-sdk');
let s3 = new AWS.S3();
let https = require('https');
let md5 = require('md5');

let parse = require('csv-parse/lib/sync');

// translate controlled fields to user fields
// key: controlled field names used in code
// value: field names in data
let field_map = {
  /*
  date_time: 'date_time',
  species: 'species',
  projectTitle: 'project',
  projectId: 'projectId',
  site: 'site',
  subSite: 'subSite',
  cameraLocation: 'cameraLocation',
  filename: 'filename',
  corrected_date_time: 'corrected_date_time',
  sex: 'sex',
  lifeStage: 'lifeStage',
  antler: 'antler',
  remarks: 'remarks',
  //*/
  date_time: '時間',
  species: '物種',
  projectTitle: '計畫名稱',
  projectId: 'projectId',
  site: '樣區',
  subSite: '子樣區',
  cameraLocation: '相機位置',
  filename: '檔名',
  corrected_date_time: '正確時間',
  sex: '性別',
  lifeStage: '年齡',
  antler: '角況',
  remarks: '備註',
}

let csv_field_label_map = {};
csv_field_label_map[field_map.species] = "物種";

// fields that are parts of metadata instead of annotation data
let not_data_fields = [
  field_map.projectTitle,
  field_map.projectId,
  field_map.site,
  field_map.subSite,
  field_map.cameraLocation,
  field_map.date_time,
  field_map.corrected_date_time,
  field_map.filename
];

// translate user fields to controlled field names
let inverse_field_map = {};
inverse_field_map[field_map.projectTitle] = 'projectTitle';
inverse_field_map[field_map.projectId] = 'projectId';
inverse_field_map[field_map.site] = 'site';
inverse_field_map[field_map.subSite] = 'subSite';
inverse_field_map[field_map.cameraLocation] = 'cameraLocation';
inverse_field_map[field_map.species] = 'species';

// these fields are required
let required_fileds = [
  field_map.site,
  field_map.cameraLocation
];

const user_password = 'camera-trap';
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
      fullRes += data;
      // console.log('Response: ' + res);
    });

    res.on('end', function(){
      if (callbackArgsOverride !== undefined) {
        post_callback(callbackArgsOverride);
      }
      else {
        post_callback(JSON.parse(fullRes));
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

    userId = tag_data.userId;

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

    // TODO: 這個 aggregate 寫差了，日後有空可改
    let post_aggregate = [
      {"$match": {"_id": tag_data.projectId}},
      {"$unwind": "$dataFieldEnabled"},
      {
        "$lookup": {
          "from": "DataFieldAvailable",
          "localField": "dataFieldEnabled",
          "foreignField": "key",
          "as": "field_details"
        }
      },
      {
        "$project": {
          "field_details": "$field_details",
          "speciesList": "$speciesList",
          "dailyTestTime": "$dailyTestTime"
        }
      },
      {"$unwind": "$field_details"},
      {
        "$project": {
          "_id":  false,
          "speciesList": "$speciesList",
          "key": "$field_details.key",
          "label": "$field_details.label",
          "widget_type": "$field_details.widget_type",
          "widget_select_options": "$field_details.widget_select_options",
          "widget_date_format": "$field_details.widget_date_format",
          "dailyTestTime": "$dailyTestTime"
        }
      }
    ];

    // 讀取 project-metadata 與欄位設定相關的資訊, 包括物種清單、其他啟用欄位與定時測試照片的時間設定測試
    post_to_api("/project/aggregate", post_aggregate, validate_and_create_json);

    let fullCameraLocation = tag_data.projectId + "/" + tag_data.site + "/" + tag_data.subSite + "/" + tag_data.cameraLocation;
    let fullCameraLocationMd5 = md5(fullCameraLocation);

    // 讀完 project-metadata 後的 callback
    function validate_and_create_json (res) {

      let speciesList = [];
      let validators = {};
      let dailyTestTime;

      console.log(res.results);
      if (res.results && res.results.length > 0) {

        // 物種清單與每日測試時間在每個 result 中重複，因此取第一個 (index 0) 即可
        // 物種清單
        if (Array.isArray(res.results[0].speciesList)) {
          speciesList = res.results[0].speciesList;
        }
        validators[field_map.species] = speciesList;

        // 每日測試時間
        if (Array.isArray(res.results[0].dailyTestTime) && res.results[0].dailyTestTime.length > 0) {
          let dailyTestTime_length = res.results[0].dailyTestTime.length;
          dailyTestTime = res.results[0].dailyTestTime[dailyTestTime_length - 1].time;
        }

        console.log(['Daily Test Time', dailyTestTime]);

        // 依欄位 mapping 找到欄位值驗證器 (目前就是 array of values)
        res.results.forEach(function(f){
          if (f.widget_type == 'select') {
            if (f.widget_select_options.length > 0) {
              if (field_map[f.key]) {
                validators[field_map[f.key]] = f.widget_select_options;
              }
            }
          }
          if (field_map[f.key]) {
            inverse_field_map[field_map[f.key]] = f.key;
            csv_field_label_map[field_map[f.key]] = f.label;
          }
        });

        console.log(validators);
      }

      console.log(['Inverse FieldMap', inverse_field_map]);

      let mma_upsert_querys;
      let mma_relative_url_json = root_dir + "json/" + upload_session_id + "/" + tag_data.userId + "/" + file_key_name_part + ".mma.json";

      let mmm_upsert_querys = [];
      let mmm_relative_url_json = root_dir + "json/" + upload_session_id + "/" + tag_data.userId + "/" + file_key_name_part + ".mmm.json";

      let mma = {};
      let mmm = {};

      let data_errors = [];

      // 讀進 CSV
      s3.getObject(params, function(err, data) {
        if (err) {
          console.log(err);
        }
        else {
          let csv_string = data.Body.toString();
          let records = parse(csv_string,
            {
              columns: true,
              trim: true,
              skip_empty_lines: true
            }
          );

          // console.log(records);
          let max_timestamp = -Infinity;
          let min_timestamp = Infinity;

          let latestDataDate = '';
          let earliestDataDate = '';

          let unmatched_metadata_exists = false;
          let unmatched_fields = [];
          let problematic_ids = new Set();
          let missing_required = false;
          let missing_required_fields = [];

          let record_keys = Object.keys(records[0]);
          required_fileds.forEach(function(rf){
            if (record_keys.indexOf(rf) == -1) {
              missing_required = true;
              missing_required_fields.push(rf);
            }
          });

          let force_import_validated = true;
          // 每筆 record 對應到一個 token，但單一多媒體檔也可能同時有多個 tokens，因此要用 unique id 為 group
          records.forEach(function (record, record_idx) {

            let multimedia_error_flag = false;

            if (!((!unmatched_metadata_exists || force_import_validated) && !missing_required)) {
              return;
            }

            let unmatched_metadata = false;

            let token_error_flag = false;
            let data = [];

            let baseFileName;
            let baseFileNameParts;
            let timestamp, corrected_timestamp;

            let date_time_obj = new Date(record[field_map.date_time] + '+8');
            timestamp = date_time_obj.getTime() / 1000;
            //console.log([record[field_map.date_time], timestamp]);

            let corrected_date_time = record[field_map.corrected_date_time] ? record[field_map.corrected_date_time] : record[field_map.date_time];
            let corrected_date_time_obj = new Date(corrected_date_time + '+8');
            corrected_timestamp = corrected_date_time_obj.getTime() / 1000;


            let year = corrected_date_time_obj.getFullYear();
            let month = corrected_date_time_obj.getMonth() + 1;
            let day = corrected_date_time_obj.getDate();
            let hour = corrected_date_time_obj.getHours();

            if (corrected_timestamp > max_timestamp) {
              max_timestamp = corrected_timestamp;
              latestDataDate = corrected_date_time;
            }
            if (corrected_timestamp < min_timestamp) {
              min_timestamp = corrected_timestamp;
              earliestDataDate = corrected_date_time;
            }

            if (dailyTestTime) {
              let dtt_re = new RegExp(dailyTestTime + "$");
              let dtt_matched = dtt_re.exec(corrected_date_time);
              if (dtt_matched) {
                record[field_map.species] = '定時測試';
              }
            }

            // 不含路徑的檔名
            baseFileName = record[field_map.filename].split("/").pop();
            baseFileNameParts = baseFileName.split(".");
            // 上傳時的原檔名
            let uploaded_baseFileName = baseFileName;

            // 副檔名
            let ext = baseFileNameParts.pop();
            // console.log(['ext', ext]);

            // 目前只接受 jpg, mp4 與 avi
            let mm_type = "Invalid";
            let typeFolder = "images"
            if (ext.match(/jpg$|jpeg$/i)) {
              ext = "jpg";
              mm_type = "StillImage";
            }
            else if (ext.match(/mp4$/i)) {
              ext = "mp4";
              mm_type = "MovingImage";
              typeFolder = "video";
            }
            else if (ext.match(/avi$/i)) {
              ext = "avi";
              mm_type = "MovingImage";
              typeFolder = "video";
            }
            else {
              // TODO: throw errer
            }

            // 檔名後方強制加上 timestamp 以確保包含完整計畫與地點的檔案路徑是系統唯一
            baseFileName = baseFileNameParts.join(".") + "_" + timestamp;
            let relocate_path = root_dir + typeFolder + "/orig/" + fullCameraLocation;
            let relative_url = relocate_path + '/' + baseFileName + "." + ext;
            let _id = md5(relative_url);

            if (!mma[_id]) mma[_id] = {$set: {tokens: []}, $setOnInsert: {}};
            if (!mmm[_id]) mmm[_id] = {$set: {}, $setOnInsert: {}};

            // console.log(record);
            // validating data
            // 驗證資料由此開始
            for (let k in record) {

              if (not_data_fields.indexOf(k) >= 0) {
                // 如果上傳時的 tags 與 資料內容不符，看如何處理...
                if (
                  record[k] &&
                  tag_data[inverse_field_map[k]] &&
                  (tag_data[inverse_field_map[k]] != record[k])) {
                  unmatched_metadata = true;
                  unmatched_metadata_exists = true;
                  unmatched_fields.push(
                    "第 `" + (record_idx + 1) + "` 行欄位 `" + k + "` 上傳設定： `" + tag_data[inverse_field_map[k]] +"`, 資料值： `" + record[k] + "`;"
                  );
                  problematic_ids.add(_id);
                  // break;
                }
                continue;
              }

              // 欄位值未通過驗證，error flag on
              let data_error_flag = false;
              if (record.hasOwnProperty(k)) {
                if (validators[k] && validators[k].indexOf(record[k]) < 0) {
                  data_error_flag = true;
                  token_error_flag = true;
                  multimedia_error_flag = true;
                }
              }

              data.push({
                key: inverse_field_map[k] || k,
                label: csv_field_label_map[k] ? csv_field_label_map[k] : k,
                value: record[k],
                data_error_flag: data_error_flag,
                last_validated_timestamp: (Date.now() / 1000),
              })
            }

            if (unmatched_metadata) {
              // TODO: throw error?
              delete mma[_id];
              delete mmm[_id];
              return;
            }

            let species_shortcut = "尚未辨識";
            if (record[field_map.species]) {
              species_shortcut = record[field_map.species];
            }

            // console.log(['SPECIES SHORTCUT', species_shortcut]);
            // set mma tokens
            mma[_id].$set.tokens.push(
              {
                data: data,
                token_error_flag: token_error_flag,
                species_shortcut: species_shortcut
              }
            );

            // for MMA access control
            mma[_id]._id = _id;
            mma[_id].projectId = tag_data.projectId;
            mma[_id].fullCameraLocationMd5 = fullCameraLocationMd5;

            // set value
            mma[_id].$set.date_time_corrected_timestamp = corrected_timestamp;
            mma[_id].$set.corrected_date_time = corrected_date_time;
            mma[_id].$set.modifiedBy = tag_data.userId;
            mma[_id].$set.type = mm_type;
            mma[_id].$set.year = year;
            mma[_id].$set.month = month;
            mma[_id].$set.day = day;
            mma[_id].$set.hour = hour;
            mma[_id].$set.imageUrlPrefix = 'https://s3-ap-northeast-1.amazonaws.com/camera-trap/';
            mma[_id].$set.multimedia_error_flag = mma[_id].$set.multimedia_error_flag || multimedia_error_flag;

            // set on insert (upsert)
            mma[_id].$setOnInsert = {
              url: relative_url,
              url_md5: _id,
              date_time_original_timestamp: timestamp,
              projectId: tag_data.projectId,
              projectTitle: tag_data.projectTitle,
              site: tag_data.site,
              subSite: tag_data.subSite,
              cameraLocation: tag_data.cameraLocation,
              fullCameraLocationMd5: fullCameraLocationMd5,
              uploaded_file_name: uploaded_baseFileName,
              timezone: "+8"
            }
            mma[_id].$addToSet = {
              related_upload_sessions: upload_session_id
            }

            // for MMM access control
            mmm[_id]._id = _id;
            mmm[_id].projectId = tag_data.projectId;
            mmm[_id].fullCameraLocationMd5 = fullCameraLocationMd5;

            // set value
            mmm[_id].$set.date_time_corrected_timestamp = corrected_timestamp;
            mmm[_id].$set.corrected_date_time = corrected_date_time;
            mmm[_id].$set.modifiedBy = tag_data.userId;
            mmm[_id].$set.type = mm_type;
            mmm[_id].$set.year = year;
            mmm[_id].$set.month = month;
            mmm[_id].$set.day = day;
            mmm[_id].$set.hour = hour;
            mmm[_id].$set.imageUrlPrefix = 'https://s3-ap-northeast-1.amazonaws.com/camera-trap/';

            // set on insert (upsert)
            mmm[_id].$setOnInsert = {
              url: relative_url,
              url_md5: _id,
              date_time_original_timestamp: timestamp,
              modify_date: "",
              device_metadata: {},
              projectId: tag_data.projectId,
              projectTitle: tag_data.projectTitle,
              site: tag_data.site,
              subSite: tag_data.subSite,
              cameraLocation: tag_data.cameraLocation,
              fullCameraLocationMd5: fullCameraLocationMd5,
              uploaded_file_name: uploaded_baseFileName,
              timezone: "+8"
            }
            mmm[_id].$addToSet = {
              related_upload_sessions: upload_session_id
            }


          }); // end of records

          // 如果缺欄位或欄位值不一致
          if (unmatched_metadata_exists) {
            let unmatched_fields_string = unmatched_fields.join('`, `');
            data_errors.push(
              "以下欄位資訊與上傳設定值不一致: " + unmatched_fields_string + "."
            )
          }

          if (missing_required) {
            let missing_required_fields_string = missing_required_fields.join('`, `');
            data_errors.push(
              "缺少以下必要欄位: `" + missing_required_fields_string + "`."
            );
          }

          // 檢查新舊資料重疊
          let overlap_range = {
            "query": {
              "date_time_corrected_timestamp": {"$gte": min_timestamp, "$lte": max_timestamp},
            }
          }

          let data_overlap = false;
          post_to_api("/media/annotation/exists", overlap_range, function(res) {
            console.log(JSON.stringify(overlap_range, null, 2));

            if (res.results !== null) {
              data_overlap = true;
              data_errors.push ("上傳資料可能與過往資料重疊。");
            }

            let modified = Date.now() / 1000;
            if (data_errors.length > 0) {
              post_to_api("/upload-session/bulk-update", [{
                _id: upload_session_id,
                projectId: tag_data.projectId,
                $set: {
                  earliestDataDate: earliestDataDate,
                  latestDataDate: latestDataDate,
                  status: "ERROR",
                  modified: modified
                },
                $push: {
                  messages: {
                    problematic_ids: Array.from(problematic_ids),
                    key: file_key,
                    errors: data_errors,
                    modified: modified
                  }
                },
                $setOnInsert: {
                  _id: upload_session_id,
                  upload_session_id: upload_session_id,
                  fullCameraLocationMd5: fullCameraLocationMd5,
                  projectTitle: tag_data.projectTitle,
                  projectId: tag_data.projectId,
                  by: tag_data.userId,
                },
                $upsert: true
              }], function(res) {
                console.log(["ERROR REPORTING", res]);
              });
            }
            else {
              post_to_api("/upload-session/bulk-update", [{
                _id: upload_session_id,
                projectId: tag_data.projectId,
                $set: {
                  earliestDataDate: earliestDataDate,
                  latestDataDate: latestDataDate,
                  status: "SUCCESS",
                  modified: modified
                },
                $setOnInsert: {
                  _id: upload_session_id,
                  upload_session_id: upload_session_id,
                  fullCameraLocationMd5: fullCameraLocationMd5,
                  projectTitle: tag_data.projectTitle,
                  projectId: tag_data.projectId,
                  by: tag_data.userId,
                },
                $upsert: true
              }], function(res) {
                console.log(["SUCCESS", res]);
              });
            }

            if ((!unmatched_metadata_exists || force_import_validated) && !missing_required) {

              mma_upsert_querys = Object.keys(mma).map(function(key) {
                return mma[key];
              });

              mmm_upsert_querys = Object.keys(mmm).map(function(key) {
                return mmm[key];
              });

              // console.log(JSON.stringify(mmm_upsert_querys, null, 2));

              let mma_op = {
                endpoint: "/media/annotation/bulk-update",
                post: mma_upsert_querys,
                fullCameraLocationMd5: fullCameraLocationMd5
              }

              let mmm_op = {
                endpoint: "/media/bulk-update",
                post: mmm_upsert_querys,
                fullCameraLocationMd5: fullCameraLocationMd5
              }

              if (data_overlap) {
                mma_op.hold = true;
                mmm_op.hold = true;
              }

              let mma_upsert_querys_string = JSON.stringify(mma_op, null, 2);
              s3.upload({Bucket: bucket, Key: mma_relative_url_json, Body: mma_upsert_querys_string, ContentType: "application/json", Tagging: tags_string}, {},
                function(err, data) {
                  if (err)
                    console.log('ERROR!');
                  else
                    console.log('OK');
                });

              let mmm_upsert_querys_string = JSON.stringify(mmm_op, null, 2);
              s3.upload({Bucket: bucket, Key: mmm_relative_url_json, Body: mmm_upsert_querys_string, ContentType: "application/json", Tagging: tags_string}, {},
                function(err, data) {
                  if (err)
                    console.log('ERROR!');
                  else
                    console.log('OK');
                });

            }
          });

        }
      });
    }
  });
};
