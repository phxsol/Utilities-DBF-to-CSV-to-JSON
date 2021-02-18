#!/usr/bin/env node

var program = require('commander');
var pkg = require('../package.json');

program
    .parse(process.argv);

if(program.args[0] == "convert"){
    program
        .command('convert [db] [design] [view] ', 'Emit data results .CSV file.')
        .version(pkg.version)
        .usage('[options] <db> <design> <view>')
        .parse(process.argv);
}

if(program.args[0] == "model"){
    program
        .command('model [object]', 'Model the raw data into cohesive objects.')
        .version(pkg.version)
        .usage('[options] <object>')
        .parse(process.argv);
}

if(program.args[0] == "xfer"){
    program
        .command('xfer [file]', 'xFer the data from a DBF table file.')
        .version(pkg.version)
        .usage('[options] <file>')
        .option('-b, --batch_size', 'Set the size of each batch.')
        .option('-s, --start_recno', 'Set the point at which to start the xfer.')
        .option('-t, --throttle', 'Set the throttle speed [slow]< 1 -- 6 >[fast].')
        .parse(process.argv);
}
