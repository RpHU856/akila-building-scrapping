const fs = require('fs');
let file = 'app.js';
let content = fs.readFileSync(file, 'utf8');

const target1 = `    const resArray = await Promise.all(reqs);

    const res = first(resArray[0]);
    if (!res) {
        setStatus("error", "Aucune donnée BDNB trouvée pour ce bâtiment.");
        return;
    }`;

const replace1 = `    const resArray = await Promise.all(reqs);

    const res = first(resArray[0]);
    if (!res) {
        setStatus("error", "Aucune donnée BDNB trouvée pour ce bâtiment.");
        return { error: true };
    }
    res.adresse_label = adresseLabel;
    res.cle_ban = cleBan;
    res.rnb_id = rnbId;`;

const target2 = `    const dataCtx = {
        rnbId, type: bType, lat, lon, codeInsee, adresseLabel, cleBan,
        res, usage, ffo, topo, prop, risque, reseau, dpe, elec, gaz,
        ademeRes, dGeo, dFcu, dSirene, dEdu, dBench, dSitadel: sitadelData,
        dvf, urba, bpe, rnc, rpls, qpv, hthd, geoSpx, zoac, topoEqu, dGpu, dDinum
    };

    renderResultsDOM(dataCtx, "single", null);
}`;

const replace2 = `    const dataCtx = {
        rnbId, type: bType, lat, lon, codeInsee, adresseLabel, cleBan,
        res, usage, ffo, topo, prop, risque, reseau, dpe, elec, gaz,
        ademeRes, dGeo, dFcu, dSirene, dEdu, dBench, dSitadel: sitadelData,
        dvf, urba, bpe, rnc, rpls, qpv, hthd, geoSpx, zoac, topoEqu, dGpu, dDinum,
        base: res // base alias for compatibility
    };

    renderResultsDOM(dataCtx, "single", null);
    return dataCtx;
}`;

content = content.replace(target1, replace1);
content = content.replace(target2, replace2);
fs.writeFileSync(file, content);
console.log("Patched fetchBuildingData returns");
