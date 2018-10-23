'use strict'

let AWS = require('aws-sdk');
let s3 = new AWS.S3();
let https = require('https');
let md5 = require('md5');

let parse = require('csv-parse');
let parser = parse({
  columns: true,
  trim: true,
  skip_empty_lines: true
});

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

    let post_aggregate_string = JSON.stringify(post_aggregate);
    let post_options = {
      host: "camera-trap.tw",
      port: '443',
      path: "/api/project-metadata/aggregate",
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      }
    };

    let post_req = https.request(post_options, function(res) {
      res.setEncoding('utf8');
      res.on('data', function (res) {
        console.log('Response: ' + res);
        validate_and_create_json(res);
        // context.succeed();
      });
      res.on('error', function (e) {
        console.log("Got error: " + e.message);
        context.done(null, 'FAILURE');
      });
    });

    // post the data
    post_req.write(post_aggregate_string);
    post_req.end();

    let species_field = "species";
    let not_data_fields = [
      "project", "site", "sub_site", "location", "date_time", "filename"
    ];
    let full_location = tag_data.project + "/" + tag_data.site + "/" + tag_data.sub_site + "/" + tag_data.location;
    let full_location_md5 = md5(full_location);

    function validate_and_create_json (res) {

      let species_list;
      let validators = {};
      let daily_test_time;

      if (res.results && res.results.length > 0) {
        species_list = res.results[0].species_list;
        
        if (species_list.length > 0) {
          validators[species_field] = species_list;
        }

        if (res.results[0].daily_test_time[0]) {
          daily_test_time = res.results[0].daily_test_time[0].time;
        }

        res.results.forEach(function(f){
          if (f.widget_type == 'select') {
            if (f.widget_select_options.length > 0) {
              validators[f.key] = f.widget_select_options;
            }
          }
        });
      }

      let mma_upsert_querys = [];
      let mma_relative_url_json = root_dir + "json/" + upload_session_id + "/" + file_key_name_part + ".mma.json";

      let mmm_upsert_querys = [];
      let mmm_relative_url_json = root_dir + "json/" + upload_session_id + "/" + file_key_name_part + ".mmm.json";

      let date_time_original_timestamp_col_num;

      let mma = {};
      let mmm = {};

      s3.getObject(params).createReadStream()
        .pipe(parser)
        .on('readable', function() {
          let record;
          
          let token_error_flag = false;
          let data = [];

          record = this.read();
          if (record) {

            let baseFileName;
            let baseFileNameParts;
            let timestamp;

            timestamp = new Date(record['date_time']).getTime() / 1000;
            baseFileNameParts = record['filename'].split(".");
            baseFileNameParts.pop();
            baseFileName = baseFileNameParts.join(".") + "_" + timestamp;
            let relocate_path = root_dir + "images/orig/" + full_location;
            let relative_url = relocate_path + '/' + baseFileName + ".jpg";
            let _id = md5(relative_url);

            if (!mma[_id]) mma[_id] = {tokens: []};

            console.log(record);
            for (let k in record) {

              if (not_data_fields.indexOf(k) >= 0) {
                // TODO: make some validation
                if (tag_data[k] && (tag_data[k] != record[k])) {
                  // TODO: throw error
                }
                continue;
              }

              let data_error_flag = false;
              if (record.hasOwnProperty(k)) {
                if (validators[k] && !validators[k].indexOf(record[k])) {
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

            mma[_id].tokens.push(
              {
                data: data,
                token_error_flag: token_error_flag
              }
            );

            mma[_id].url_md5 = _id;
            mma[_id].project = tag_data.project;
            mma[_id].full_location_md5 = full_location_md5;
            mma[_id].url = relative_url;
            mma[_id].date_time_original_timestamp = timestamp;

          }
        })
        .on('end', function(){
          console.log('END');
          console.log(JSON.stringify(mma, null, 2));
          context.succeed();
        });
    }
  });

}