const fs = require('fs');
let file = 'app.js';
let lines = fs.readFileSync(file, 'utf-8').split('\n');

let startIndex = -1;
let endIndex = -1;
let count = 0;

for (let i = 0; i < lines.length; i++) {
    if (lines[i].includes('const sitadel = dataCtx.sitadel || [];')) {
        count++;
        if (count === 1) { // We remove the FIRST occurrence
            startIndex = i;
            // Now find the end of the block
            for (let j = i; j < lines.length; j++) {
                if (lines[j].includes('sitadelSection.style.display = "none";')) {
                    endIndex = j + 1; // including the '    }' if any
                    if (lines[endIndex] && lines[endIndex].includes('}')) {
                        endIndex++;
                    }
                    break;
                }
            }
            break;
        }
    }
}

if (startIndex !== -1 && endIndex !== -1) {
    // Remove lines from startIndex-1 (// SITADEL comment) to endIndex
    lines.splice(startIndex - 1, endIndex - startIndex + 1);
    fs.writeFileSync(file, lines.join('\n'));
    console.log('Removed block successfully');
} else {
    console.log('Could not find block or already removed');
}
