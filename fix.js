const fs = require('fs');
let txt = fs.readFileSync('app.js', 'utf8');

// The exact string to locate
const START = 'const newCode = `\n';
const END = "fs.writeFileSync('builder.js', newCode);";

let s = txt.indexOf(START);
let e = txt.indexOf(END) + END.length;

if (s === -1 || e === -1) {
    console.error("Tags not found", s, e);
    process.exit(1);
}

let codeStr = txt.substring(s, e);
let correct = codeStr
    .replace(START, '')
    .replace("`;\nconst fs = require('fs');\n" + END, '');

correct = correct.replace(/\\\\\`/g, '`').replace(/\\\\\$/g, '$');

txt = txt.substring(0, s) + correct + txt.substring(e);
fs.writeFileSync('app.js', txt);
console.log("Success");
