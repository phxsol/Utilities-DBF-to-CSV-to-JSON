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

var scratch_pad = {
    srcDocDBs:  [],
    tokens: [],
    patterns: [],
    phrases: []
};
var modelsDocDB = nano.use('phx_models');

modelsDocDB.list(function(err, data){
    try {
        if (!err) {
            var keys = data.rows.map(mapKeys, "tokens_patterns_phrases");
            scratch_pad.srcDocDBs = keys;

            modelsDocDB.fetch({"keys": keys}, function (err, data) {
                if (!err) {
                    var docDB_cnt = data.rows.length;
                    var curr_docDB_num = 0;
                    data.rows.forEach(function (doc) {
                        curr_docDB_num++;
                        console.log("Tokens: " + scratch_pad.tokens.length + " collected.");
                        console.log("Patterns: " + scratch_pad.patterns.length + " collected.");
                        console.log("Phrases: " + scratch_pad.phrases.length + " collected.");
                        console.log("");
                        if(!doc.doc){console.error(JSON.stringify(doc))};
                        if (doc.doc && doc.doc.srcFields) {
                        console.log("opening " + doc.key + " [ " + curr_docDB_num + " of " + docDB_cnt + " ] | " + doc.doc.srcFields.length + " phrases identified.");
                            doc.doc.srcFields.forEach(function (srcField) {
                                var phrase = srcField[0];
                                var ph_ndx = scratch_pad.phrases.findIndex(meThisPhrase, phrase);
                                if(ph_ndx==-1){
                                    scratch_pad.phrases.push({
                                        phrase: phrase,
                                        use_cases: [{
                                            docKey: doc.key,
                                            srcFld: [srcField[0]],
                                            uses: 1
                                        }]
                                    });
                                } else {
                                    var docKey_ndx = scratch_pad.phrases[ph_ndx].use_cases.findIndex(meThisDocKey, doc.key);
                                    if(docKey_ndx==-1){

                                        scratch_pad.phrases[ph_ndx].use_cases.push({
                                            docKey: doc.key,
                                            srcFld: [srcField[0]],
                                            uses: 1
                                        });
                                    } else {
                                        scratch_pad.phrases[ph_ndx].use_cases[docKey_ndx].srcFld.push(srcField[0]);
                                        scratch_pad.phrases[ph_ndx].use_cases[docKey_ndx].uses += 1;
                                    }
                                }

                                var xTokens = phrase.split("_");
                                xTokens.forEach(function (token) {
                                    token = (typeof(scratch_pad.tokens[token])=="function" || typeof(scratch_pad.tokens[token])=="number") ? "phx_" + token : token;
                                    var pt_ndx = scratch_pad.tokens.findIndex(meThisToken, token);
                                    if(pt_ndx==-1) {
                                        scratch_pad.tokens.push({
                                            token: token,
                                            use_cases: [{
                                                docKey: doc.key,
                                                srcFld: [srcField[0]],
                                                uses: 1
                                            }]
                                        });
                                    } else {
                                        var docKey_ndx = scratch_pad.tokens[pt_ndx].use_cases.findIndex(meThisDocKey, doc.key);
                                        if(docKey_ndx==-1){

                                            scratch_pad.tokens[pt_ndx].use_cases.push({
                                                docKey: doc.key,
                                                srcFld: [srcField[0]],
                                                uses: 1
                                            });
                                        } else {
                                            scratch_pad.tokens[pt_ndx].use_cases[docKey_ndx].srcFld.push(srcField[0]);
                                            scratch_pad.tokens[pt_ndx].use_cases[docKey_ndx].uses += 1;
                                        }
                                    }
                                });

                                var xTokens = srcField[0].split("_");
                                var phrase_size = xTokens.length;
                                for(var pattern_size=2;pattern_size<=phrase_size;pattern_size++) {
                                    for (var patt_tokn_cnt = 1; patt_tokn_cnt <= pattern_size && patt_tokn_cnt < phrase_size; patt_tokn_cnt++) {
                                        var token_positions = [];
                                        for (var prime_tokn_pos = 1; prime_tokn_pos <= (phrase_size - patt_tokn_cnt + 1); prime_tokn_pos++) {
                                            for (var tokn_pos_ndx = 0; tokn_pos_ndx < patt_tokn_cnt; tokn_pos_ndx++) {
                                                var tokn_pos = prime_tokn_pos + tokn_pos_ndx;
                                                token_positions[tokn_pos_ndx] = tokn_pos;
                                            }
                                            var pattern = [];
                                            for (var patt_ndx = 0; patt_ndx < pattern_size; patt_ndx++) {
                                                var tokn_pos = patt_ndx + 1;
                                                if(token_positions.find(isThisATokenSlot, tokn_pos)){
                                                    pattern.push(xTokens[patt_ndx]);
                                                } else {
                                                    pattern.push("*");
                                                }
                                            }


                                            var pattern_mask = pattern.join(" ");
                                            var ndx_of_pattern = scratch_pad.patterns.findIndex(findPattern, pattern_mask);
                                            if(ndx_of_pattern==-1){
                                                scratch_pad.patterns.push({
                                                    pattern: pattern_mask,
                                                    use_cases: [{
                                                        docKey: doc.key,
                                                        srcFld: [srcField[0]],
                                                        uses: 1
                                                    }]
                                                });
                                            } else {
                                                var docKey_ndx = scratch_pad.patterns[ndx_of_pattern].use_cases.findIndex(meThisDocKey, doc.key);
                                                if(docKey_ndx==-1){

                                                    scratch_pad.patterns[ndx_of_pattern].use_cases.push({
                                                        docKey: doc.key,
                                                        srcFld: [srcField[0]],
                                                        uses: 1
                                                    });
                                                } else {
                                                    scratch_pad.patterns[ndx_of_pattern].use_cases[docKey_ndx].srcFld.push(srcField[0]);
                                                    scratch_pad.patterns[ndx_of_pattern].use_cases[docKey_ndx].uses += 1;
                                                }
                                            }
                                        }
                                    }
                                }
                            });
                        }
                    });
                    modelsDocDB.head("tokens_patterns_phrases", function(err,data){
                        if(!err){
                            scratch_pad._rev = data.rev;
                        }
                        modelsDocDB.insert(scratch_pad, "tokens_patterns_phrases", function(err, data){
                            if(!err){

                            }
                        });
                    });
                    var a = 1;
                } else console.err(err);
            });
        } else console.err(err);
    } catch(err) {
        console.log(scratch_pad);
    }
});

function mapKeys(element){
    if(element.key!=this) {
        return element.key;
    }
}

// uniqueSrcCnt:    Calculate the unique tables in which this is used.
// homeSrc:         The docDB which possesses an imbalanced proportion of the total usages... if one exists for this token.
function mapSignificance(element, index, array){
    var srcDocDBs = [];
    element.forEach(function(useCase){
        var docDBName = useCase.docDB;
        if(srcDocDBs.indexOf(docDBName)==-1){
            srcDocDBs[docDBName] = 1;
        }
    });
    element.uniqueSrcCnt = srcDocDBs.length;
}

function isThisATokenSlot(element, index, array){
    return (element==this);
}

function findPattern(element, index, array){
    return(element.pattern==this);
}

function meThisPhrase(element, index, array){
    return(element && element.phrase==this);
}

function meThisToken(element, index, array){
    return(element && element.token==this);
}

function meThisDocKey(element, index, array){
    return(element.docKey==this);
}
