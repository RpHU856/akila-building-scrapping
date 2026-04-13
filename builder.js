async function fetchBuildingData(rnbId, type, adresseLabel, cleBan) {
    const res = await resoudreEntree(rnbId);
    const batId = res.bat_id_bdnb;
    if (!batId) return { res, error: "BDNB missing" };
    
    const p1 = { batiment_groupe_id: `eq.${batId}`, limit: 1 };
    const pm = { batiment_groupe_id: `eq.${batId}`, order: "millesime.desc", limit: 4 };

    const [dBase, dUsage, dFfo, dTopo, dProp, dRisque, dReseau, dDpeT, dDpeP, dElec, dGaz] = await Promise.all([
        bdnbQuery("batiment_groupe", { ...p1, select: "code_commune_insee,libelle_commune_insee,code_iris,s_geom_groupe" }),
        bdnbQuery("batiment_groupe_synthese_propriete_usage", { ...p1, select: "usage_principal_bdnb_open" }),
        bdnbQuery("batiment_groupe_ffo_bat", { ...p1, select: "annee_construction,mat_mur_txt,mat_toit_txt,usage_niveau_1_txt,nb_niveau,nb_log" }),
        bdnbQuery("batiment_groupe_bdtopo_bat", { ...p1, select: "hauteur_mean,altitude_sol_mean,l_usage_1" }),
        bdnbQuery("batiment_groupe_proprietaire", { ...p1, select: "bat_prop_denomination_proprietaire" }),
        bdnbQuery("batiment_groupe_risques", { ...p1, select: "alea_argile,alea_radon,alea_sismique" }),
        bdnbQuery("batiment_groupe_indicateur_reseau_chaud_froid", { ...p1, select: "indicateur_distance_au_reseau,reseau_en_construction" }),
        bdnbQuery("batiment_groupe_dpe_tertiaire", { ...p1, select: "identifiant_dpe,classe_conso_energie_dpe_tertiaire,classe_emission_ges_dpe_tertiaire,conso_dpe_tertiaire_ep_m2,emission_ges_dpe_tertiaire_m2,type_energie_chauffage,date_etablissement_dpe,surface_utile" }),
        bdnbQuery("batiment_groupe_dpe_representatif_logement", { ...p1, select: "identifiant_dpe,classe_bilan_dpe,classe_emission_ges,conso_5_usages_ep_m2,emission_ges_5_usages_m2,type_energie_chauffage,date_etablissement_dpe,surface_habitable_immeuble" }),
        bdnbQuery("batiment_groupe_dle_elec_multimillesime", { ...pm, select: "millesime,nb_pdl_tot,conso_tot" }),
        bdnbQuery("batiment_groupe_dle_gaz_multimillesime", { ...pm, select: "millesime,nb_pdl_tot,conso_tot" }),
    ]);

    const base = first(dBase), usage = first(dUsage), ffo = first(dFfo),
          topo = first(dTopo), prop = first(dProp), risque = first(dRisque),
          reseau = first(dReseau);

    // Dynamic resolution of the active DPE
    let dpe = null, bType = type;
    const hasT = dDpeT && dDpeT.length > 0;
    const hasP = dDpeP && dDpeP.length > 0;

    if (hasT && hasP) {
        dpe = type === "T" ? dDpeT[0] : dDpeP[0]; // Respect requested type if both are available
        bType = type;
    } else if (hasT) {
        dpe = dDpeT[0];
        bType = "T";
    } else if (hasP) {
        dpe = dDpeP[0];
        bType = "P";
    }
    
    // Add bType into the returned object so that rendering isn't locked by the global setting
    const elecs = dElec?.length ? dElec : [{millesime: "2022"}], gazs = dGaz?.length ? dGaz : [{millesime: "2022"}];
    const elec = elecs[0], gaz = gazs[0];

    const ademe = await collecterADEME(rnbId, res.adresse_label || adresseLabel, res.cle_ban || cleBan, bType, val(dpe?.identifiant_dpe));

    return {
        res, base, usage, ffo, topo, prop, risque, reseau, dpe, elec, gaz, elecs, gazs, ademe: ademe.resultats, bType
    };
}

async function runMultiScan() {
    if (selectedRnbIds.size === 0) return;
    
    const entreeInput = $("input-entree").value.trim();
    const entree = entreeInput || Array.from(selectedRnbIds)[0];
    
    showState("loading-state");
    setStatus("loading", "Processing Scan...");
    
    try {
        setStep(2);
        
        const rnbIds = Array.from(selectedRnbIds);
        const buildings = [];
        let firstAddressingRes = null;
        
        // Fetch all independently
        for (const rnbId of rnbIds) {
            const bData = await fetchBuildingData(rnbId, currentType, firstAddressingRes?.adresse_label, firstAddressingRes?.cle_ban);
            if (bData.res && bData.res.adresse_label && !firstAddressingRes) {
                firstAddressingRes = bData.res;
            }
            buildings.push(bData);
        }

        const validBuildings = buildings.filter(b => !b.error);
        if (validBuildings.length === 0) {
            showState("error-state");
            $("error-title").textContent = "BDNB Extraction Failed";
            $("error-message").textContent = "None of the selected buildings were found in BDNB.";
            setStatus("error", "Error");
            return;
        }

        setStep(3);
        
        // Build global data
        const global = {
            count: validBuildings.length,
            base: { ...validBuildings[0].base },
            usage: { ...validBuildings[0].usage },
            ffo: { ...validBuildings[0].ffo },
            topo: { ...validBuildings[0].topo },
            prop: { ...validBuildings[0].prop },
            risque: { ...validBuildings[0].risque },
            reseau: { ...validBuildings[0].reseau },
            dpe: { ...validBuildings[0].dpe },
            elec: { ...validBuildings[0].elec },
            gaz: { ...validBuildings[0].gaz },
            ademe: []
        };
        
        // Sum properties for global
        global.base.s_geom_groupe = 0;
        global.ffo.nb_log = 0;
        global.dpe.surface_utile = 0;
        global.dpe.surface_habitable_immeuble = 0;
        
        const gElecs = {};
        const gGazs = {};
        
        const ademeSeen = new Set();
        for (const b of validBuildings) {
            if (b.base?.s_geom_groupe) global.base.s_geom_groupe += Number(b.base.s_geom_groupe);
            if (b.ffo?.nb_log) global.ffo.nb_log += Number(b.ffo.nb_log);
            if (b.bType === "T" && b.dpe?.surface_utile) global.dpe.surface_utile += Number(b.dpe.surface_utile);
            if (b.bType === "P" && b.dpe?.surface_habitable_immeuble) global.dpe.surface_habitable_immeuble += Number(b.dpe.surface_habitable_immeuble);
            
            if (b.elecs) { b.elecs.forEach(e => { if (e.millesime) gElecs[e.millesime] = (gElecs[e.millesime]||0) + Number(e.conso_tot||0); }); }
            if (b.gazs) { b.gazs.forEach(g => { if (g.millesime) gGazs[g.millesime] = (gGazs[g.millesime]||0) + Number(g.conso_tot||0); }); }
            
            for(const a of b.ademe) {
                const k = a.numero_dpe || a.Numero_DPE || Math.random();
                if(!ademeSeen.has(k)) { ademeSeen.add(k); global.ademe.push(a); }
            }
        }

        global.elecs = Object.entries(gElecs).map(([m, c]) => ({ millesime: m, conso_tot: c })).sort((a,b)=>b.millesime.localeCompare(a.millesime));
        global.gazs = Object.entries(gGazs).map(([m, c]) => ({ millesime: m, conso_tot: c })).sort((a,b)=>b.millesime.localeCompare(a.millesime));
        
        // Clear pseudo-ids on global so we don't display a specific ID incorrectly
        global.dpe.identifiant_dpe = "—";
        global.base.code_commune_insee = validBuildings[0].base.code_commune_insee;
        
        scanData = {
            adresse: entreeInput || validBuildings[0].res.adresse_label || "Selected Zone",
            firstRes: validBuildings[0].res,
            mode: validBuildings.length > 1 ? "multi" : "single",
            global: global,
            buildings: validBuildings,
            genere_le: new Date().toISOString()
        };

        // Render Tabs and the corresponding data
        showState("results-container");
        renderResultsTabs();

        // Success
        setStatus("", "Scan complete");
        addToHistory(entree, currentType);
        showToast("Scan complete!", "success");

    } catch(e) {
        console.error("Scan error:", e);
        showState("error-state");
        $("error-title").textContent = "Unexpected Error";
        $("error-message").textContent = e.message || "An unexpected error occurred.";
        setStatus("error", "Error");
    }

    $("btn-search").disabled = false;
}

function renderResultsTabs() {
    const tabsContainer = $("results-tabs");
    tabsContainer.innerHTML = "";
    
    if (scanData.mode === "multi") {
        tabsContainer.style.display = "block";
        const btnGlobal = document.createElement("button");
        btnGlobal.textContent = "Global Preview";
        btnGlobal.onclick = () => {
            Array.from(tabsContainer.children).forEach(b => b.classList.remove("active"));
            btnGlobal.classList.add("active");
            renderResultsDOM(scanData.global, "global", scanData.firstRes);
        };
        tabsContainer.appendChild(btnGlobal);
        
        scanData.buildings.forEach((b, i) => {
            const btnB = document.createElement("button");
            btnB.textContent = `Bat ${i+1} : ${b.res.adresse_label || b.res.rnb_id || 'Unknown'}`;
            btnB.onclick = () => {
                Array.from(tabsContainer.children).forEach(btn => btn.classList.remove("active"));
                btnB.classList.add("active");
                renderResultsDOM(b, "single", b.res);
            };
            tabsContainer.appendChild(btnB);
        });
        
        // Click global by default
        btnGlobal.click();
    } else {
        tabsContainer.style.display = "none";
        renderResultsDOM(scanData.buildings[0], "single", scanData.buildings[0].res);
    }
}

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

    // Risks
    clearAndAppendRows("data-risks", [
        ["Clay / Shrink-Swell", val(risque?.alea_argile)],
        ["Radon", val(risque?.alea_radon)],
        ["Seismic", val(risque?.alea_sismique)],
        ...(reseau?.indicateur_distance_au_reseau ? [["Heat Network (distance)", val(reseau?.indicateur_distance_au_reseau)]] : []),
        ...(reseau?.reseau_en_construction !== undefined ? [["Network Under Construction", val(reseau?.reseau_en_construction)]] : []),
    ]);

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
