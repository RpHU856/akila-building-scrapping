const fs = require('fs');
let txt = fs.readFileSync('app.js', 'utf8');

const sIdx = txt.indexOf('async function runMultiScan()');
const eIdx = txt.indexOf('// ── EXPORT JSON');

if (sIdx !== -1 && eIdx !== -1) {
    const newCode = fs.readFileSync('builder.js', 'utf8');
    txt = txt.substring(0, sIdx) + newCode + "\n" + txt.substring(eIdx);
    fs.writeFileSync('app.js', txt);
    console.log("SUCCESS injected builder");
} else {
    console.error("FAIL indexes not found", sIdx, eIdx);
    process.exit(1);
}
