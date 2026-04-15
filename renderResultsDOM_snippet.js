function renderResultsDOM(dataCtx, mode, resObj) {
    const isGlobal = mode === "global";
    
    // Unpack context depending on what we passed (either scanData.global or a building object bData)
    const base = dataCtx.base;
    const usage = dataCtx.usage;
    const ffo = dataCtx.ffo;
    const topo = dataCtx.topo;
    const prop = dataCtx.prop;
    const risque = dataCtx.risque;
    const reseau = dataCtx.reseau;
    const dpe = dataCtx.dpe;
    const elec = dataCtx.elec;
    const gaz = dataCtx.gaz;
    const ademe = dataCtx.ademe;

    // Banner
    const rType = isGlobal ? currentType : (dataCtx.bType || currentType);
    const adresseAffichee = isGlobal ? `Global Preview – ${dataCtx.count} ${rType === "T" ? "Commercial" : "Residential"} Buildings Selected` : (resObj.adresse_label || resObj.rnb_id || "Building Result");
    $("result-address").textContent = adresseAffichee;
    $("badge-type").textContent = rType === "T" ? "Commercial" : "Residential";

    const etiquette = rType === "T" ? val(dpe?.classe_conso_energie_dpe_tertiaire) : val(dpe?.classe_bilan_dpe);
    const dpeBadge = $("badge-dpe");
    dpeBadge.textContent = isGlobal ? `Average/Peak DPE: ${etiquette}` : (`DPE ${etiquette}`);
    dpeBadge.className = `badge badge-dpe${etiquette !== "—" ? ` dpe-${etiquette.toUpperCase()}` : ""}`;

    // IDs
    if (isGlobal || !resObj) {
        $("section-ids").style.display = "none";
    } else {
        $("section-ids").style.display = "";
        const rnbLink = resObj.rnb_id ? `https://rnb.beta.gouv.fr/carte?q=${resObj.rnb_id}${resObj.lat ? `&coords=${resObj.lat},${resObj.lon},18` : ''}` : "—";
        clearAndAppendRows("data-ids", [
            ["RNB ID (Building)", val(resObj.rnb_id)],
            ["BDNB ID (CSTB)", val(resObj.bat_id_bdnb)],
            ["BAN Key (Interop)", val(resObj.cle_ban)],
            ...(resObj.rnb_id ? [["RNB Record", rnbLink]] : [])
        ]);
    }

    // Location
    clearAndAppendRows("data-location", [
        ["Commune (Code)", `${val(base?.libelle_commune_insee)} (${val(base?.code_commune_insee)})`],
        ["IRIS Code", val(base?.code_iris)],
        ...(!isGlobal ? [
            ["Coordinates (Lat, Lon)", resObj?.lat ? `${resObj.lat}, ${resObj.lon}` : "—"]
        ] : [])
    ]);

    // Physical
    const volStr = (base?.s_geom_groupe && topo?.hauteur_mean) ? Math.round(base.s_geom_groupe * topo.hauteur_mean) : null;
    clearAndAppendRows("data-physical", [
        ["Construction Year", val(ffo?.annee_construction)],
        ["Footprint Area", base?.s_geom_groupe ? `${base.s_geom_groupe} m²` : "—"],
        ["Levels (Count)", val(ffo?.nb_niveau)],
        ["Habit. / Usable Area", rType === "P" 
            ? (dpe?.surface_habitable_immeuble ? `${dpe.surface_habitable_immeuble} m²` : "—")
            : (dpe?.surface_utile ? `${dpe.surface_utile} m²` : "—")
        ],
        ["Building Volume (est.)", volStr ? `${volStr} m³` : "—"],
        ["Height (mean)", topo?.hauteur_mean ? `${topo.hauteur_mean} m` : "—"],
        ["Elevation (Ground)", topo?.altitude_sol_mean ? `${topo.altitude_sol_mean} m` : "—"],
        ["Wall Material", val(ffo?.mat_mur_txt)],
        ["Roof Material", val(ffo?.mat_toit_txt)]
    ]);

    // Usage
    clearAndAppendRows("data-usage", [
        ["Primary Usage", isGlobal ? "Mixed / Multiple" : val(usage?.usage_principal_bdnb_open)],
        ["Ground Floor Usage", val(ffo?.usage_niveau_1_txt)],
        ["Topo Usage", val(topo?.l_usage_1)],
        ["Housing Count", isGlobal ? (ffo?.nb_log || "—") : val(ffo?.nb_log)],
        ["Owner Denomination", val(prop?.bat_prop_denomination_proprietaire)]
    ]);

    function renderEnergyScale(id, label, valueStr, scaleClass) {
        const container = $(id);
        container.innerHTML = "";
        if (valueStr === "—" || !valueStr) return;
        const value = valueStr.toUpperCase();
        const letters = ["A", "B", "C", "D", "E", "F", "G"];
        if (!letters.includes(value)) return;
        
        letters.forEach(letter => {
            const span = document.createElement("span");
            span.className = `energy-class ${scaleClass}-${letter}${letter === value ? " active" : ""}`;
            span.textContent = letter;
            container.appendChild(span);
        });
    }

    function renderConsoSection(title, data, iconKey, emoji) {
        const section = document.createElement("div");
        section.className = "consumption-section";
        section.innerHTML = `<h4>${emoji} ${title}</h4>`;

        if (!data || !data.length || typeof data[0] !== 'object' || !data[0].conso_tot) {
            section.innerHTML += '<p style="color:var(--text-muted);font-size:0.82rem;padding-left:4px;">Not available in open data.</p>';
            $("data-consumption").appendChild(section);
            return;
        }

        const maxConso = Math.max(...data.map(d => parseFloat(d.conso_tot) || 0));

        for (const rowData of data) {
            const c = parseFloat(rowData.conso_tot) || 0;
            const pct = maxConso > 0 ? (c / maxConso * 100) : 0;
            const row = document.createElement("div");
            row.className = "conso-row";
            const cost = iconKey === "elec" ? `~${Math.round(c * 0.15 / 1000).toLocaleString("en-US")} k€/yr` : "";
            row.innerHTML = `
                <span class="conso-year">${rowData.millesime || "—"}</span>
                <div class="conso-bar-bg"><div class="conso-bar ${iconKey}" style="width:0%"></div></div>
                <span class="conso-value">${Math.round(c).toLocaleString("en-US")} kWh</span>
                <span class="conso-cost">${cost}</span>
            `;
            section.appendChild(row);

            setTimeout(() => {
                const bar = row.querySelector(".conso-bar");
                if (bar) bar.style.width = `${pct}%`;
            }, 50);
        }
        
        $("data-consumption").appendChild(section);
    }

    $("data-energy").innerHTML = "";
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
    }

    $("data-consumption").innerHTML = "";
    renderConsoSection("Electricity", dataCtx.elecs, "elec", "⚡");
    renderConsoSection("Gas", dataCtx.gazs, "gaz", "🔥");

    // ── REGULATORY ALERTS ──────────────────────────────────────────────────
    const alertsList = computeAlerts(dataCtx);
    const alertsSection = $("section-alerts");
    const alertsDiv = $("data-alerts");
    alertsDiv.innerHTML = "";
    if (alertsList.length > 0) {
        alertsSection.style.display = "";
        alertsList.forEach(a => {
            const card = document.createElement("div");
            card.className = `alert-card ${a.level}`;
            card.innerHTML = `
                <div class="alert-icon">${a.icon}</div>
                <div class="alert-body">
                    <div class="alert-title">${a.title}</div>
                    <div class="alert-desc">${a.desc}</div>
                    ${a.deadline ? `<span class="alert-deadline">${a.deadline}</span>` : ""}
                </div>`;
            alertsDiv.appendChild(card);
        });
    } else {
        alertsSection.style.display = "none";
    }

    // Risks (BDNB + Géorisques enrichi) — keys aligned with Python
    const geo = dataCtx.geo || {};
    clearAndAppendRows("data-risks", [
        ["Clay / RGA (BDNB)",            val(risque?.alea_argile)],
        ["Clay Hazard (Géorisques)",     val(geo.argile_alea)],
        ["Radon (BDNB)",                 val(risque?.alea_radon)],
        ["Radon Class (Géorisques)",     geo.radon_classe ? `Class ${geo.radon_classe}` : "—"],
        ["Seismic Zone",                 geo.sismique_zone ? `Zone ${geo.sismique_zone} — ${geo.sismique_lib}` : val(risque?.alea_sismique)],
        ["Flood Zones (AZI)",            geo.inondation_nb_zones !== undefined ? `${geo.inondation_nb_zones} zone(s) — ${geo.inondation_detail}` : "—"],
        ["Underground Cavities (500m)",  geo.cavites_nb !== undefined ? `${geo.cavites_nb}${geo.cavites_nb > 0 ? ` — ${geo.cavites_types}` : ""}` : "—"],
        ["Natural Disasters (CatNat)",   geo.catnat_nb !== undefined ? `${geo.catnat_nb} event(s) — ${geo.catnat_types || "—"}` : "—"],
        ["ICPE Facilities (500m)",        geo.icpe_rayon_500m !== undefined ? `${geo.icpe_rayon_500m}${geo.icpe_rayon_500m > 0 ? ` — ${geo.icpe_noms}` : ""}` : "—"],
    ]);

    // France Chaleur Urbaine — keys aligned with Python (fcu_ prefix)
    const fcu = dataCtx.fcu;
    const fcuSection = $("section-fcu");
    if (fcu) {
        fcuSection.style.display = "";
        clearAndAppendRows("data-fcu", [
            ["Eligible for Connection", fcu.fcu_eligible ? "✓ YES" : "✗ No"],
            ["Distance to Network",     `${fcu.fcu_distance_m} m`],
            ["Network Name",            val(fcu.fcu_reseau_nom)],
            ["Network ID",              val(fcu.fcu_reseau_id)],
            ["Renewable Energy %",      fcu.fcu_enr_pct !== "—" ? `${fcu.fcu_enr_pct}%` : "—"],
            ["CO₂ Emission",            fcu.fcu_co2 !== "—" ? `${fcu.fcu_co2} kg/MWh` : "—"],
            ...(reseau?.indicateur_distance_au_reseau ? [["BDNB Distance", val(reseau.indicateur_distance_au_reseau)]] : []),
            ...(reseau?.reseau_en_construction !== undefined ? [["Under Construction", val(reseau.reseau_en_construction)]] : []),
        ]);
    } else {
        if (reseau?.indicateur_distance_au_reseau || reseau?.reseau_en_construction !== undefined) {
            fcuSection.style.display = "";
            clearAndAppendRows("data-fcu", [
                ["Heat Network Distance (BDNB)", val(reseau?.indicateur_distance_au_reseau)],
                ["Réseau en Construction",        val(reseau?.reseau_en_construction)],
            ]);
        } else {
            fcuSection.style.display = "none";
        }
    }

    // Benchmark ADEME
    const bench = dataCtx.benchmark || {};
    const benchSection = $("section-benchmark"), benchDiv = $("data-benchmark");
    benchDiv.innerHTML = "";
    if (bench.commune || bench.activite) {
        benchSection.style.display = "";
        if (bench.commune) {
            const cm = bench.commune;
            const row = document.createElement("div");
            row.className = "ademe-card";
            row.innerHTML = `<div class="ademe-header"><span class="ademe-numdpe">Commune average</span><span class="ademe-addr">${cm.libelle_commune || ""}</span></div><div class="ademe-detail">${cm.conso_m2_kwh || "—"} kWh/m²/yr</div>`;
            benchDiv.appendChild(row);
        }
        if (bench.activite) {
            bench.activite.slice(0, 3).forEach(act => {
                const row = document.createElement("div");
                row.className = "ademe-card";
                row.innerHTML = `<div class="ademe-header"><span class="ademe-numdpe">By activity</span><span class="ademe-addr">${act.libelle_activite || ""}</span></div><div class="ademe-detail">Median: ${act.conso_m2_kwh_mediane || "—"} kWh/m²/yr</div>`;
                benchDiv.appendChild(row);
            });
        }
    } else {
        benchSection.style.display = "none";
    }

    // SIRENE
    const sirene = dataCtx.sirene || [];
    const sireneSection = $("section-sirene"), sireneDiv = $("data-sirene");
    sireneDiv.innerHTML = "";
    if (sirene.length) {
        sireneSection.style.display = "";
        sirene.forEach(e => {
            const card = document.createElement("div");
            card.className = "ademe-card";
            card.innerHTML = `<div class="ademe-header"><span class="ademe-numdpe">${e.siret}</span><span class="ademe-addr">${e.nom}</span></div><div class="ademe-detail">NAF: ${e.naf_code} — ${e.naf_libelle || e.naf_lib || "—"} · Staff: ${e.effectif} · Status: ${e.statut}${e.adresse && e.adresse !== "—" ? "<br>" + e.adresse : ""}</div>`;
            sireneDiv.appendChild(card);
        });
    } else {
        sireneSection.style.display = "none";
    }

    // Annuaire Éducation
    const edu = dataCtx.education || [];
    const eduSection = $("section-education"), eduDiv = $("data-education");
    eduDiv.innerHTML = "";
    if (edu.length) {
        eduSection.style.display = "";
        edu.forEach(e => {
            const card = document.createElement("div");
            card.className = "ademe-card";
            const siret = e.siren_siret ? `SIRET: ${e.siren_siret}` : "";
            const contact = [e.telephone, e.mail].filter(x => x && x !== "None").join(" · ");
            const extras = [e.hebergement ? "🛏 Boarding" : null, e.restauration ? "🍽 Canteen" : null, e.apprentissage ? "🔧 Apprenticeship" : null].filter(Boolean).join(" · ");
            card.innerHTML = `<div class="ademe-header"><span class="ademe-numdpe">UAI: ${e.identifiant_de_l_etablissement || "—"}</span><span class="ademe-addr">${e.nom_etablissement || "—"} (${e.libelle_nature || e.type_etablissement || "—"})</span></div><div class="ademe-detail">${e.adresse_1 || ""}, ${e.code_postal || ""} ${e.nom_commune || ""} · ${e.statut_public_prive || "—"}${siret ? " · " + siret : ""}${contact ? "<br>" + contact : ""}${extras ? "<br>" + extras : ""}</div>`;
            eduDiv.appendChild(card);
        });
    } else {
        eduSection.style.display = "none";
    }

    // DVF
    const dvf = dataCtx.dvf || [];
    const dvfSection = $("section-dvf"), dvfDiv = $("data-dvf");
    dvfDiv.innerHTML = "";
    if (dvf.length) {
        dvfSection.style.display = "";
        dvf.slice(0, 5).forEach(mut => {
            const prix = mut.valeur_fonciere ? parseInt(mut.valeur_fonciere).toLocaleString("fr-FR") + " €" : "—";
            const card = document.createElement("div");
            card.className = "ademe-card";
            card.innerHTML = `<div class="ademe-header"><span class="ademe-numdpe">${mut.date_mutation || "—"}</span><span class="ademe-addr">${mut.type_local || "—"} — ${mut.surface_reelle_bati || "—"} m²</span></div><div class="ademe-detail">Price: ${prix}</div>`;
            dvfDiv.appendChild(card);
        });
    } else {
        dvfSection.style.display = "none";
    }

    // SITADEL
    const sitadel = dataCtx.sitadel || [];
    const sitadelSection = $("section-sitadel"), sitadelDiv = $("data-sitadel");
    sitadelDiv.innerHTML = "";
    if (sitadel.length) {
        sitadelSection.style.display = "";
        sitadel.forEach(pc => {
            const card = document.createElement("div");
            card.className = "ademe-card";
            card.innerHTML = `<div class="ademe-header"><span class="ademe-numdpe">${pc.date_autorisation || "—"}</span><span class="ademe-addr">${pc.type_autorisation || "—"}</span></div><div class="ademe-detail">${pc.libelle_destination_principale || "—"}</div>`;
            sitadelDiv.appendChild(card);
        });
    } else {
        sitadelSection.style.display = "none";
    }

    // ADEME
    const ademeSection = $("section-ademe");
    const ademeData = $("data-ademe");
    if (ademe?.length) {
        ademeSection.style.display = "";
        ademeData.innerHTML = "";
        for (const d of ademe.slice(0, 4)) {
            const addrParts = [d.numero_rue, d.type_voie, d.nom_rue, d.Adresse_Brute].filter(x => x && x !== "\\N" && x !== "None");
            const addr = addrParts.join(" ");
            const cp = champ(d, "code_postal", "Code_Postal_BAN");
            const commune = champ(d, "commune", "Nom_Commune_BAN");
            const ndpe = champ(d, "numero_dpe", "Numero_DPE");
            const etiqE = champ(d, "classe_consommation_energie", "Etiquette_DPE");
            const etiqG = champ(d, "classe_estimation_ges", "Etiquette_GES");
            const secteur = champ(d, "secteur_activite", "Secteur_activite_principale_batiment", "Type_batiment");

            const card = document.createElement("div");
            card.className = "ademe-card";
            card.innerHTML = `
                <div class="ademe-header">
                    <span class="ademe-numdpe">N° ${ndpe}</span>
                    <span class="ademe-addr">${addr} ${cp} ${commune}</span>
                </div>
                <div class="ademe-detail">Energy: ${etiqE}  ·  GHG: ${etiqG}  ·  Sector: ${secteur}</div>
            `;
            ademeData.appendChild(card);
        }
    } else {
        ademeSection.style.display = "none";
    }

    // Observatory
    const obsSection = $("section-observatory");
    const obsData = $("data-observatory");
    let numeroDPE = val(dpe?.identifiant_dpe);
    if (!isGlobal && numeroDPE !== "—") {
        obsSection.style.display = "";
        obsData.innerHTML = `
            <a class="observatory-link" href="https://observatoire-dpe-audit.ademe.fr/pub/dpe/${numeroDPE}" target="_blank" rel="noopener">
                <div class="obs-icon">
                    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M18 13v6a2 2 0 01-2 2H5a2 2 0 01-2-2V8a2 2 0 012-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>
                </div>
                <div class="obs-info">
                    <h4>View full DPE on the ADEME Observatory</h4>
                    <p>${numeroDPE}</p>
                </div>
            </a>
        `;
    } else {
        obsSection.style.display = "none";
    }

    // Raw Data (Rayon X)
    const rawContainer = $("data-raw");
    rawContainer.innerHTML = "";
    if (isGlobal) {
        const row = document.createElement("div");
        row.className = "raw-row";
        row.innerHTML = `<span class="raw-key">info</span><span class="raw-val">Raw data hidden in global preview. Click on a specific building to view.</span>`;
        rawContainer.appendChild(row);
    } else {
        const allData = { ...base, ...usage, ...ffo, ...topo, ...dpe, ...risque, ...reseau };
        const exclude = new Set(["geom_groupe", "geom_groupe_pos_wgs84", "geom_cstr", "geom_adresse"]);
        const sortedKeys = Object.keys(allData).filter(k => !exclude.has(k) && allData[k] !== null && allData[k] !== "" && allData[k] !== "None").sort();
        for (const key of sortedKeys) {
            const row = document.createElement("div");
            row.className = "raw-row";
            row.innerHTML = `<span class="raw-key">${key}</span><span class="raw-val">${val(allData[key])}</span>`;
            rawContainer.appendChild(row);
        }
    }
}

