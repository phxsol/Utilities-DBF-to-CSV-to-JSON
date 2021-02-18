#!/usr/bin/env node
const fs = require('fs');
const path = require('path');
const program = require('commander');
const pkg = require('../package.json');
const colors = require('colors');
const oledb = require('edge-oledb');
const async = require('async');
const agentkeepalive = require('agentkeepalive');
const crc = require('crc');
const nano = require('nano')({
    "url": "http://[SECURE_URL_CREDENTIALS]",
    "requestDefaults": {
        "agent": new agentkeepalive({
            maxSockets: 50,
            maxKeepAliveRequests: 0,
            maxKeepAliveTime: 30000
        })
    }
});
const batch_size = 30030;

var fieldNames = null;

var docDB, db_name, design_name, view_name;



/**********************************************************************************************************************/
// Object Definitions //
/**********************************************************************************************************************/



/**********************************************************************************************************************/
// Routines //
/**********************************************************************************************************************/

function proc_init() {
    try {
        program .command('convert [db] [design] [view] ', 'Emit data results .CSV file.')
            .version(pkg.version)
            .usage('[options] <db> <design> <view>')
            .action(function (db, design, view) {
                db_name = db.toLowerCase();
                design_name = design;
                view_name = view;
            })
            .parse(process.argv);

        modelsDocDB = nano.use("phx_models");
        if(view_name == "_all_docs"){
            docDB = nano.use(db_name);

            modelsDocDB.get(db_name, function(err,response){
                if(!err) {
                    var xfieldNames = response.srcFields.map(pullOutNames);
                    fieldNames = [].concat("key", xfieldNames);
                    console.log("\"" + fieldNames.join("\",\"") + "\"");

                    extractDataBatch(design_name, view_name, { limit: batch_size, skip: 0 });
                }
            });
        } else {
            docDB = nano.use(db_name);

            nano.request({ db: db_name,
                ddoc: db_name,
                method: 'get',
                params: { rev: rev }
            }, callback);

            docDB.view(db_name, view_name, function(err,response){
                if(!err) {
                    var xfieldNames = response.srcFields.map(pullOutNames);
                    fieldNames = [].concat("key", xfieldNames);
                    console.log("\"" + fieldNames.join("\",\"") + "\"");

                    extractDataBatch(design_name, view_name, { limit: batch_size, skip: 0 });
                }
            });
        }

    } catch(err) {
        console.error(err);
        process.exit(1);
    }
}






function extractDataBatch(designDoc, viewName, params){
    docDB.view(designDoc, viewName, params, function(err, data) {
        if(!err){

            var csvExports = data.rows.map(emitCSVRecords);
            if(params.skip + batch_size < data.total_rows){
                params.skip += batch_size;
                console.error("Batch Complete | Next: [ " + params.skip + " to " + (params.skip + batch_size) + " ]");
                extractDataBatch(designDoc, viewName, params);
            } else {
                console.error("Conversion Complete.")
            }
        } else {
            console.error("Extraction failure: failed between record " + params.skip + " and " + params.skip + batch_size);
            process.exit(1);
        }
    });
}



/**********************************************************************************************************************/
// Sub-Routines //
/**********************************************************************************************************************/

function emitCSVRecords(element, index, array){
    var values = [];

    for(var key in fieldNames){
        if(key==0){
            values.push(element.key);
        } else {
            var fieldName = fieldNames[key];
            if (element.value.hasOwnProperty(fieldName)) {
                values.push(element.value[fieldName]);
            } else {
                values.push("");
            }
        }
    }
    console.log("\"" + values.join("\",\"") + "\"");
}

function pullOutNames(element, index, array){
    return element[0];
}

/**********************************************************************************************************************/
// Main Script Entry //
/**********************************************************************************************************************/

proc_init();






/**********************************************************************************************************************/
// Deprecated Logic //
/**********************************************************************************************************************/
