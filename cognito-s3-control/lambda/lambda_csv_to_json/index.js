'use strict'

let AWS = require('aws-sdk');
let s3 = new AWS.S3();
let https = require('https');
let md5 = require('md5');

let parse = require('csv-parse/lib/sync');

let field_map = {
  date_time: 'date_time',
  species: 'species',
  project: 'project',
  site: 'site',
  sub_site: 'sub_site',
  location: 'location',
  filename: 'filename',
  corrected_date_time: 'corrected_date_time',
  sex: 'sex',
  'life-stage': 'life_stage',
  antler: 'antler'
}

let not_data_fields = [
  field_map.project, 
  field_map.site, 
  field_map.sub_site, 
  field_map.location, 
  field_map.date_time, 
  field_map.corrected_date_time, 
  field_map.filename
];

let inverse_field_map = {};
inverse_field_map[field_map.project] = 'project';
inverse_field_map[field_map.site] = 'site';
inverse_field_map[field_map.sub_site] = 'sub_site';
inverse_field_map[field_map.location] = 'location';

function post_to_api (endpoint_path, json, post_callback) {
  let post_options = {
    host: "camera-trap.tw",
    port: '443',
    path: endpoint_path,
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    }
  };

  let post_req = https.request(post_options, function(res) {
    res.setEncoding('utf8');
    res.on('data', function (res) {
      // console.log('Response: ' + res);
      post_callback(JSON.parse(res));
      // context.succeed();
    });
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

    let post_aggregate = {
      "aggregate": [
        {"$match": {"_id": tag_data.project}},
        {"$unwind": "$data_field_enabled"},
        {
          "$lookup": {
            "from": "data-fields-available",
            "localField": "data_field_enabled",
            "foreignField": "key",
            "as": "field_details"
          }
        },
        {
          "$project": {
            "field_details": "$field_details",
            "species_list": "$species_list",
            "daily_test_time": "$daily_test_time"
          }
        },
        {"$unwind": "$field_details"},
        {
          "$project": {
            "_id":  false,
            "species_list": "$species_list",
            "key": "$field_details.key",
            "widget_type": "$field_details.widget_type",
            "widget_select_options": "$field_details.widget_select_options",
            "widget_date_format": "$field_details.widget_date_format",
            "daily_test_time": "$daily_test_time"
          }
        }
      ]
    }

    post_to_api("/api/project-metadata/aggregate", post_aggregate, validate_and_create_json);
  
    let full_location = tag_data.project + "/" + tag_data.site + "/" + tag_data.sub_site + "/" + tag_data.location;
    let full_location_md5 = md5(full_location);

    function validate_and_create_json (res) {

      let species_list;
      let validators = {};
      let daily_test_time;

      if (res.results && res.results.length > 0) {
        species_list = res.results[0].species_list;
        
        if (species_list.length > 0) {
          validators[field_map.species] = species_list;
        }

        if (res.results[0].daily_test_time[0]) {
          daily_test_time = res.results[0].daily_test_time[0].time;
        }

        res.results.forEach(function(f){
          if (f.widget_type == 'select') {
            if (f.widget_select_options.length > 0) {
              if (field_map[f.key]) {
                validators[field_map[f.key]] = f.widget_select_options;
              }
            }
          }
        });

        console.log(validators);
      }

      let mma_upsert_querys;
      let mma_relative_url_json = root_dir + "json/" + upload_session_id + "/" + file_key_name_part + ".mma.json";

      let mmm_upsert_querys = [];
      let mmm_relative_url_json = root_dir + "json/" + upload_session_id + "/" + file_key_name_part + ".mmm.json";

      let date_time_original_timestamp_col_num;

      let mma = {};
      let mmm = {};

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
          records.forEach(function (record) {
            // 每筆 record 就是一個 token          

            let token_error_flag = false;
            let data = [];

            let baseFileName;
            let baseFileNameParts;
            let timestamp, corrected_timestamp;

            timestamp = new Date(record[field_map.date_time]).getTime() / 1000;
            console.log([record[field_map.date_time], timestamp]);
            
            let corrected_date_time = record[field_map.corrected_date_time] ? record[field_map.corrected_date_time] : record[field_map.date_time];
            corrected_timestamp = new Date(corrected_date_time).getTime() / 1000;
            if (corrected_timestamp > max_timestamp) max_timestamp = corrected_timestamp;
            if (corrected_timestamp < min_timestamp) min_timestamp = corrected_timestamp;

            if (daily_test_time) {
              let dtt_re = new RegExp(daily_test_time + "$");
              let dtt_matched = dtt_re.exec(corrected_date_time);
              if (dtt_matched) {
                record[field_map.species] = '定時測試';
              }
            }

            baseFileNameParts = record[field_map.filename].split(".");

            let ext = baseFileNameParts.pop();
            // console.log(['ext', ext]);
            
            let mm_type = "Invalid";
            if (ext.match(/jpg$|jpeg$/i)) {
              ext = "jpg";
              mm_type = "StillImage";
            }
            else if (ext.match(/mp4$/i)) {
              ext = "mp4";
              mm_type = "MovingImage";
            }
            else {
              // TODO: throw errer
            }

            baseFileName = baseFileNameParts.join(".") + "_" + timestamp;
            let relocate_path = root_dir + "images/orig/" + full_location;
            let relative_url = relocate_path + '/' + baseFileName + "." + ext;
            let _id = md5(relative_url);

            if (!mma[_id]) mma[_id] = {$set: {tokens: []}, $setOnInsert: {}};

            // console.log(record);
            // validating data
            let unmatched_metadata = false;
            for (let k in record) {

              if (not_data_fields.indexOf(k) >= 0) {
                // TODO: make some validation
                if (tag_data[inverse_field_map[k]] && (tag_data[inverse_field_map[k]] != record[k])) {
                  unmatched_metadata = true;
                  break;
                }
                continue;
              }

              let data_error_flag = false;
              if (record.hasOwnProperty(k)) {
                if (validators[k] && validators[k].indexOf(record[k]) < 0) {
                  data_error_flag = true;
                  token_error_flag = true;
                }
              }

              data.push({
                key: k,
                value: record[k],
                data_error_flag: data_error_flag,
                last_validated_timestamp: (Date.now() / 1000),
              })
            }

            if (unmatched_metadata) {
              // TODO: throw error?
              delete mma[_id];
              return;
            }

            mma[_id].$set.tokens.push(
              {
                data: data,
                token_error_flag: token_error_flag
              }
            );

            // for access control
            mma[_id]._id = _id;
            mma[_id].project = tag_data.project;
            mma[_id].full_location_md5 = full_location_md5;

            // set value
            mma[_id].$set.date_time_corrected_timestamp = corrected_timestamp;
            mma[_id].$set.modified_by = tag_data.user_id;
            mma[_id].$set.type = mm_type;

            // set on insert (upsert)
            mma[_id].$setOnInsert = {
              url: relative_url,
              url_md5: _id,
              date_time_original_timestamp: timestamp,
              project: tag_data.project,
              site: tag_data.site,
              sub_site: tag_data.sub_site,
              location: tag_data.location,
              full_location_md5: full_location_md5,
              timezone: "+8"
            }
            mma[_id].$addToSet = {
              related_upload_sessions: upload_session_id
            }
          });

          let overlap_range = {
            "query": {
              "date_time_corrected_timestamp": {"$gte": min_timestamp},
              "date_time_corrected_timestamp": {"$lte": min_timestamp},
            }
          }



          //*/
          mma_upsert_querys = Object.keys(mma).map(function(key) {
            return mma[key];
          });

          console.log(JSON.stringify(mma_upsert_querys, null, 2));
          
          let mma_op = {
            endpoint: "/multimedia-annotations/bulk-update",
            post: mma_upsert_querys
          }

          let mma_upsert_querys_string = JSON.stringify(mma_op, null, 2);

          s3.upload({Bucket: bucket, Key: mma_relative_url_json, Body: mma_upsert_querys_string, ContentType: "application/json", Tagging: tags_string}, {},
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