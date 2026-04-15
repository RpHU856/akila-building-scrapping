const fs = require('fs');

const file_path = 'd:\\projects\\DPE Scrapping\\app.js';
let text = fs.readFileSync(file_path, 'utf-8');

const target1 = `    // Location
    clearAndAppendRows("data-location", [
        ["Commune (Code)", \`\${val(base?.libelle_commune_insee)} (\${val(base?.code_commune_insee)})\`],
        ["IRIS Code", val(base?.code_iris)],
        ...(!isGlobal ? [
            ["Coordinates (Lat, Lon)", resObj?.lat ? \`\${resObj.lat}, \${resObj.lon}\` : "—"]
        ] : [])
    ]);`;

const replacement1 = `    // Location
    const qpv = dataCtx.qpv;
    const geoSpx = dataCtx.geoSpx;
    clearAndAppendRows("data-location", [
        ["Commune (Code)", \`\${val(base?.libelle_commune_insee)} (\${val(base?.code_commune_insee)})\`],
        ["IRIS Code", val(base?.code_iris)],
        ["QPV (Quartier Prior)", qpv && val(qpv.quartier_prioritaire) !== "—" && val(qpv.quartier_prioritaire) !== "false" ? val(qpv.nom_quartier) : "—"],
        ["GeoSpx Reliability", geoSpx && geoSpx.fiabilite_adresse ? geoSpx.fiabilite_adresse : "—"],
        ...(!isGlobal ? [
            ["Coordinates (Lat, Lon)", resObj?.lat ? \`\${resObj.lat}, \${resObj.lon}\` : "—"]
        ] : [])
    ]);`;

text = text.replace(target1, replacement1);

const target2 = `    $("data-energy").innerHTML = "";
    if (rType === "T") {
        clearAndAppendRows("data-energy", [
            ["DPE ID", isGlobal ? "—" : val(dpe?.identifiant_dpe)],
            ["Establishment Date", val(dpe?.date_etablissement_dpe)],
            ["Tertiary Energy Perf.", val(dpe?.classe_conso_energie_dpe_tertiaire)],
            ["Tertiary GHG Emissions", val(dpe?.classe_emission_ges_dpe_tertiaire)],
            ["Energy (EP m²)", val(dpe?.conso_dpe_tertiaire_ep_m2)],
            ["GHG (m²)", val(dpe?.emission_ges_dpe_tertiaire_m2)],
            ["Heating Energy", val(dpe?.type_energie_chauffage)]
        ]);
        renderEnergyScale("energy-labels", "Energy Label", val(dpe?.classe_conso_energie_dpe_tertiaire), "scale-dpe");
    } else {
        clearAndAppendRows("data-energy", [
            ["DPE ID", isGlobal ? "—" : val(dpe?.identifiant_dpe)],
            ["Establishment Date", val(dpe?.date_etablissement_dpe)],
            ["Energy Cons. Class", val(dpe?.classe_bilan_dpe)],
            ["GHG Class", val(dpe?.classe_emission_ges)],
            ["Energy (EP m²)", val(dpe?.conso_5_usages_ep_m2)],
            ["GHG (m²)", val(dpe?.emission_ges_5_usages_m2)],
            ["Heating Energy", val(dpe?.type_energie_chauffage)]
        ]);
        renderEnergyScale("energy-labels", "Energy Label", val(dpe?.classe_bilan_dpe), "scale-dpe");
    }`;

const replacement2 = `    $("data-energy").innerHTML = "";
    if (rType === "T") {
        clearAndAppendRows("data-energy", [
            ["DPE ID", isGlobal ? "—" : val(dpe?.identifiant_dpe)],
            ["Establishment Date", val(dpe?.date_etablissement_dpe)],
            ["Tertiary Energy Perf.", val(dpe?.classe_conso_energie_dpe_tertiaire)],
            ["Tertiary GHG Emissions", val(dpe?.classe_emission_ges_dpe_tertiaire)],
            ["Energy (EP m²)", val(dpe?.conso_dpe_tertiaire_ep_m2)],
            ["GHG (m²)", val(dpe?.emission_ges_dpe_tertiaire_m2)],
            ["Heating Energy", val(dpe?.type_energie_chauffage)],
            ["Conso Chauffage", dpe?.conso_chauffage ? \`\${dpe.conso_chauffage} kWh\` : "—"],
            ["Conso Refroidissement", dpe?.conso_refroidissement ? \`\${dpe.conso_refroidissement} kWh\` : "—"],
            ["Conso ECS", dpe?.conso_ecs ? \`\${dpe.conso_ecs} kWh\` : "—"],
            ["Conso Éclairage", dpe?.conso_eclairage ? \`\${dpe.conso_eclairage} kWh\` : "—"]
        ]);
        renderEnergyScale("energy-labels", "Energy Label", val(dpe?.classe_conso_energie_dpe_tertiaire), "scale-dpe");
    } else {
        clearAndAppendRows("data-energy", [
            ["DPE ID", isGlobal ? "—" : val(dpe?.identifiant_dpe)],
            ["Establishment Date", val(dpe?.date_etablissement_dpe)],
            ["Energy Cons. Class", val(dpe?.classe_bilan_dpe)],
            ["GHG Class", val(dpe?.classe_emission_ges)],
            ["Energy (EP m²)", val(dpe?.conso_5_usages_ep_m2)],
            ["GHG (m²)", val(dpe?.emission_ges_5_usages_m2)],
            ["Heating Energy", val(dpe?.type_energie_chauffage)],
            ["Conso Chauffage", dpe?.conso_chauffage ? \`\${dpe.conso_chauffage} kWh\` : "—"],
            ["Déperditions Murs", dpe?.deperdition_mur ? \`\${dpe.deperdition_mur} W/K\` : "—"],
            ["Déperditions Toiture", dpe?.deperdition_plancher_haut || dpe?.deperdition_toiture ? \`\${dpe.deperdition_plancher_haut || dpe.deperdition_toiture} W/K\` : "—"],
            ["Déperditions Plancher Bas", dpe?.deperdition_plancher_bas ? \`\${dpe.deperdition_plancher_bas} W/K\` : "—"],
            ["Déperditions Baies Vitrées", dpe?.deperdition_baie_vitree ? \`\${dpe.deperdition_baie_vitree} W/K\` : "—"]
        ]);
        renderEnergyScale("energy-labels", "Energy Label", val(dpe?.classe_bilan_dpe), "scale-dpe");
    }`;

text = text.replace(target2, replacement2);
fs.writeFileSync(file_path, text, 'utf-8');
console.log("Replaced");
