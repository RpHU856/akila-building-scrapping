const fs = require('fs');
let file = 'd:/projects/DPE Scrapping/app.js';
let text = fs.readFileSync(file, 'utf-8');
const target = '["Details", val(dinum.desc)]';
const replacement = '["Details", dinum.isStub ? `<a href="https://api.gouv.fr/les-api/api-entreprise" target="_blank" style="color:var(--akila-blue);text-decoration:none;">Requiert Habilitation &rarr;</a>` : val(dinum.desc)]';
if (text.includes(target)) {
    text = text.replace(target, replacement);
    fs.writeFileSync(file, text);
    console.log('Replaced DINUM link');
} else {
    console.log('Target not found');
}
