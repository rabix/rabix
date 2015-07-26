#!/usr/bin/env nodejs

process.stdin.setEncoding('utf8');

var incoming = "";

process.stdin.on('readable', function() {
  var chunk = process.stdin.read();
    if (chunk !== null) {
        incoming += chunk;
    }
});

process.stdin.on('end', function() {
    var j = JSON.parse(incoming);
    var exp = ""

    if (j.script[0] == "{") {
        exp = "{return function()" + j.script + "();}";
    }
    else {
        exp = "{return " + j.script + ";}";
    }

    var fn = '';

    if (j.engineConfig) {
        for (var index = 0; index < j.engineConfig.length; ++index) {
            fn += j.engineConfig[index] + "\n";
        }
    }

    fn += "var $job = " + JSON.stringify({"inputs": j.job, "allocatedResources": {"mem": 100, "cpu": 1}}) + ";\n";
    fn += "var $self = " + JSON.stringify(j.context) + ";\n"

    fn += "(function()" + exp + ")()";
    process.stdout.write(JSON.stringify(require("vm").runInNewContext(fn, {})));
});

