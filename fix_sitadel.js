const fs = require('fs');
let file = 'd:/projects/DPE Scrapping/app.js';
let text = fs.readFileSync(file, 'utf-8');
const target = `    // SITADEL
    const sitadel = dataCtx.sitadel || [];
    const sitadelSection = $("section-sitadel"), sitadelDiv = $("data-sitadel");
    sitadelDiv.innerHTML = "";
    if (sitadel.length) {
        sitadelSection.style.display = "";
        sitadel.forEach(pc => {
            const card = document.createElement("div");
            card.className = "ademe-card";
            card.innerHTML = \`<div class="ademe-header"><span class="ademe-numdpe">\${pc.date_autorisation || "—"}</span><span class="ademe-addr">\${pc.type_autorisation || "—"}</span></div><div class="ademe-detail">\${pc.libelle_destination_principale || "—"}</div>\`;
            sitadelDiv.appendChild(card);
        });
    } else {
        sitadelSection.style.display = "none";
    }`;

if (text.includes(target)) {
    text = text.replace(target, '');
    fs.writeFileSync(file, text);
    console.log('Removed duplicate SITADEL block');
} else {
    console.log('Target not found');
}
