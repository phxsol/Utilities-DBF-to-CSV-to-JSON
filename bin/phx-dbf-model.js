const fs = require('fs');
const path = require('path');
const program = require('commander');
const pkg = require('../package.json');
const colors = require('colors');
const oledb = require('edge-oledb');
const async = require('async');
const request = require('request');
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
const prettyjson = require('prettyjson');
const batch_size = 2310;
const date_min = 1451631600000;
const date_max = Date.now();

var orderDocDB = nano.use("order");
var phxPartsDocDB = nano.use("phx_parts");

var phxParts;



/**********************************************************************************************************************/
// Object Definitions //
/**********************************************************************************************************************/

function Part(partNumber){
    this.partNumber = partNumber;
    this.Orders = [];
}

function part_Orders(drawingNumber, drawingRev){
    this.drawingNumber = drawingNumber;
    this.drawingRev = drawingRev;
    this.orders = [];
}

function Order(orderNumber){
    this.orderNumber = orderNumber;
}


function Order(){

}

function XFerProcess(timeStamp){
    this.consoleLog = [];
    this.exceptions = [];
    this.batches = [];
    this.srcLastModified = null;
    this.successful = false;
    this.succCnt = 0;
    this.failCnt = 0;
    this.unchCnt = 0;
    this.wtfeCnt = 0;
    this.complete = false;
    this.proc_start = timeStamp;
    this.proc_stop = null;
}

/**********************************************************************************************************************/
// Routines //
/**********************************************************************************************************************/

function proc_init() {
    try {
        program.command('model [object]', 'Model the raw data into cohesive objects.')
            .version(pkg.version)
            .usage('[options] <object>')
            .action(function (f) {
                modelName = f;
            })
            .parse(process.argv);

        modelParts();
        //fetchOrders(0);
    } catch(err) {
        //proc_error(err);
        //proc_exit(1);
        process.exit(1);
    }
}

function modelParts(){
    try {
        // orders table is the source for all unique part ID's
        orderDocDB.view("orders", "per_part_num", { group: true }, function(err, data) {
            if (!err) {
                var partRecords = data.rows.map(mapPartRecords);
                var partNums = partRecords.map(mapPartNumbers);

                phxPartsDocDB.fetchRevs({"keys": partNums}, function (err, data) {
                    if(!err){
                        data = (!data.rows) ? JSON.parse(data) : data;
                        phxParts = data;
                        var emptySet = (data.rows.length == 0);
                        var recsToAdd = (!emptySet) ? partRecords.filter(isolateRecordsToAdd, data.rows) : [];
                        var recsToUpdate = (!emptySet) ? partRecords.filter(isolateRecordsToUpdate, data.rows) : [];
                        var recsToSkip = (!emptySet) ? partRecords.filter(isolateRecordsToSkip, data.rows) : [];

                        phxPartsDocDB.bulk({docs: recsToAdd}, function (err, data) {
                            if(!err){
                                var emptySet = (data.length == 0);
                                var insertSuccs = (!emptySet) ? data.filter(tally_successes) : [];
                                var insertFails = (!emptySet) ? data.filter(tally_failures) :[];
                            }
                            phxPartsDocDB.bulk({docs: recsToUpdate}, function (err, data) {
                                if(!err){
                                    var emptySet = (data.length == 0);
                                    var insertSuccs = (!emptySet) ? data.filter(tally_successes) : [];
                                    var insertFails = (!emptySet) ? data.filter(tally_failures) :[];
                                }
                                phxPartsDocDB.fetch({"keys": partNums}, function (err, data) {
                                    if(!err){

                                    }
                                });
                            });

                        });
                    } else {
                        process.exit(1);
                    }
                });
            }
        });
    } catch(err) {
        process.exit(1);
    }
}

function fetchOrders(recsToSkip){

    try {
        orderDocDB.view("orders", "by_part_num", { limit: batch_size, skip: recsToSkip }, function(err, data) {
            if (!err) {

                var partRecords = data.rows.map(mapPartRecords);
                var partNums = partRecords.map(mapPartNumbers);

                phxPartsDocDB.fetch({"keys": partNums}, function (err, data) {
                    if(!err){
                        data = (!data.rows) ? JSON.parse(data) : data;
                        var emptySet = (data.rows.length == 0);

                        partRecords = (!emptySet) ? partRecords.map(isolateRecordsToAdd, data.rows) : [];
                        var recsToUpdate = (!emptySet) ? partRecords.filter(isolateRecordsToUpdate, data.rows) : [];

                        phxPartsDocDB.bulk({docs: recsToAdd}, function (err, data) {
                            if(!err){
                                var emptySet = (data.length == 0);
                                var insertSuccs = (!emptySet) ? data.filter(tally_successes) : [];
                                var insertFails = (!emptySet) ? data.filter(tally_failures) :[];


                            }
                            phxPartsDocDB.bulk({docs: recsToUpdate}, function (err, data) {
                                if(!err){
                                    var emptySet = (data.length == 0);
                                    var insertSuccs = (!emptySet) ? data.filter(tally_successes) : [];
                                    var insertFails = (!emptySet) ? data.filter(tally_failures) :[];
                                }
                                fetchOrders(0);
                            });

                        });
                    } else {process.exit(1);}
                });
            }
        });
    }
    catch(err) {
        proc_error(err);
    }

}

function modelBatch(pDocsManifest){
    ordersDocDB.fetch({"keys":pDocsManifest}, function(err, data) {
        if(!err){

            ajcards_withinWindow.concat(data.rows.map(cardsWithinWindow));

            if(ajcard_fetchListKeys.length > 0) {
                var nextBatch = ajcard_fetchListKeys.splice(0, batch_size);
                modelBatch(nextBatch);
            } else {
                nowWeAreHere();
            }

       }
    });
}

function nowWeAreHere(){
    var a = 1;
}

function proc_statusUpdate(status, postNow){
    console.log("}=> proc_statusUpdate");
    console.log(status);
    xFerProc = (!xFerProc) ? new XFerProcess(Date.now()) : xFerProc;
    xFerProc.consoleLog.push(new XFerLogEntry(Date.now(), status));

    if(postNow) {
        xFerProcessDocDB.get("inProc_modelProc", {revs_info: true}, function (err, data) {
            var singleton_xFerProc = xFerProc;
            if (!err) {
                singleton_xFerProc._rev = data._rev;
            }
            xFerProcessDocDB.insert(singleton_xFerProc, "inProc_xFerProc", function (err, data) {
                if (err) {
                    console.error("proc_statusUpdate Exception");
                    console.error(err + " : " + err.message);
                    xFerProc.consoleLog.push(new XFerLogEntry(Date.now(), "proc_statusUpdate Exception"));
                    xFerProc.consoleLog.push(new XFerLogEntry(Date.now(), err, err.message));
                    xFerProc.exceptions.push(err);
                }
            });
        });
    }
}

function proc_error(err) {
    proc_error(err, null);
}

function proc_error(err, comment){
    console.log("}=> proc_statusUpdate");
    if(comment) console.log(comment);
    console.error(err, err.message);
    xFerProc = (!xFerProc) ? new XFerProcess(Date.now()) : xFerProc;
    xFerProc.consoleLog.push(new XFerLogEntry(Date.now(), status));

    xFerProcessDocDB.get("inProc_xFerProc", {revs_info: true}, function(err, data){
        var singleton_xFerProc = xFerProc;
        if(!err){
            singleton_xFerProc._rev = data._rev;
        }
        xFerProcessDocDB.insert(singleton_xFerProc, "inProc_xFerProc", function(err, data){
            if(err){
                console.error("proc_error Exception");
                console.error(err + " : " + err.message);
                xFerProc.consoleLog.push(new XFerLogEntry(Date.now(), "proc_error Exception"));
                xFerProc.consoleLog.push(new XFerLogEntry(Date.now(), err, err.message));
                xFerProc.exceptions.push(err);
            }
        });
    });
}

function proc_exit(exitCode) {
    xFerProcessDocDB.get("inProc_xFerProc", {revs_info: true}, function(err, data){
        var singleton_xFerProc = xFerProc;
        if (!err) {
            singleton_xFerProc._rev = data._rev;
        }
        xFerProcessDocDB.insert(singleton_xFerProc, "inProc_xFerProc", function (err, data) {
            if (err) {
                console.error("proc_statusUpdate Exception");
                console.error(err + " : " + err.message);
                xFerProc.consoleLog.push(new XFerLogEntry(Date.now(), "proc_statusUpdate Exception"));
                xFerProc.consoleLog.push(new XFerLogEntry(Date.now(), err, err.message));
                xFerProc.exceptions.push(err);
            }

            xFerProcessDocDB.get(oleDB_tableName, {revs_info: true}, function(err, data){
                if(err) {
                    xFerProcLog = (xFerProcLog) ? new XFerProcLog() : xFerProcLog;
                } else {
                    xFerProcLog = data;
                }
                xFerProcLog.procHistory.push(xFerProc);
                xFerProcessDocDB.insert(xFerProcLog, oleDB_tableName, function(err){
                    if(err){
                        console.log("Well, the xFerProcLog never made it to the safehouse... See you around Chuck.")
                        console.error(err + " : " + err.message);
                    }

                    process.exit(exitCode);
                })
            });
        });
    });
}



/**********************************************************************************************************************/
// Sub-Routines //
/**********************************************************************************************************************/

function mapOrdersToParts(item, index, array){
    var a = 1;
}

function mapPartRecords(order_byPartNumber){
    var partRecord = {
        _id: (order_byPartNumber.key.length == 0) ? "[NA]" : order_byPartNumber.key,
        orderCount: order_byPartNumber.value
    };
    return partRecord;
}

function mapPartNumbers(item){
    return item._id;
}

function isolateRecordsToAdd(item, index){
    if (this[index].error && this[index].error == "not_found")  {
        return true;
    }
    if (this[index].value && this[index].value.deleted) {
        return true;
    }
}

function isolateRecordsToUpdate(item, index) {
    if (this[index].doc) {
        item._rev = this[index].value.rev;
        return true;
    }
}

function tally_failures(item){
    return(item.error);
}

function tally_successes(item){
    return(!item.error);
}

function cardsWithinWindow(currValue, index, array){
    var valAsDate = Date.parse(currValue.doc.aj_run_date);
    return (valAsDate < date_max && valAsDate > date_min);
}

/**********************************************************************************************************************/
// Main Script Entry //
/**********************************************************************************************************************/

proc_init();
