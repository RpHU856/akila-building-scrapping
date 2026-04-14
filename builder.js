async function apiFetch(url, params = {}) {
    try {
        const qs = new URLSearchParams(params).toString();
        const r = await fetch(qs ? `${url}?${qs}` : url, { headers: { "Accept": "application/json" } });
        return r.ok ? r.json() : null;
    } catch { return null; }
}

async function collecterGeoriques(lat, lon, codeInsee) {
    if (!lat || !lon) return {};
    const geo = (url, p) => apiFetch(url, p);
    const base = "https://georisques.gouv.fr/api/v1";
    const [dArgile, dSismo, dAzi, dCavites, dIcpe, dCatnat, dRadon] = await Promise.all([
        geo(`${base}/argiles`,                { latlon: `${lon},${lat}`, rayon: 100 }),
        geo(`${base}/zonage-sismique`,          { latlon: `${lon},${lat}`, rayon: 100 }),
        geo(`${base}/azi`,                      { latlon: `${lon},${lat}`, rayon: 200 }),
        geo(`${base}/cavites`,                  { latlon: `${lon},${lat}`, rayon: 500 }),
        geo(`${base}/installations-classees`,   { latlon: `${lon},${lat}`, rayon: 500 }),
        codeInsee ? geo(`${base}/gaspar/catnat`,{ code_insee_commune: codeInsee, page: 1, page_size: 5 }) : Promise.resolve(null),
        codeInsee ? geo(`${base}/radon`,        { code_insee: codeInsee }) : Promise.resolve(null),
    ]);
    const r = {};
    if (dArgile?.data?.[0]) { r.argile_alea = dArgile.data[0].lib_risque_jo || dArgile.data[0].code_alea || "—"; r.argile_code = dArgile.data[0].code_alea || "—"; }
    if (dSismo?.data?.[0])  { r.sismique_zone = dSismo.data[0].zone || "—"; r.sismique_lib = dSismo.data[0].lib_zone || "—"; }
    if (dRadon?.data?.[0])  r.radon_classe = dRadon.data[0].classe_potentiel || "—";
    r.inondation_nb = dAzi?.data?.length || 0;
    r.inondation_detail = dAzi?.data?.slice(0,3).map(x => x.lib_type_alea || x.typeAlea || "").join(", ") || "Aucune zone";
    r.cavites_nb = dCavites?.data?.length || 0;
    r.cavites_types = [...new Set((dCavites?.data || []).slice(0,5).map(x => x.typeCavite || ""))].join(", ") || "—";
    r.icpe_nb = dIcpe?.data?.length || 0;
    r.icpe_noms = (dIcpe?.data || []).slice(0,3).map(x => x.raisonSociale || x.nomEtab || "").join(", ") || "—";
    r.catnat_nb = dCatnat?.total || dCatnat?.data?.length || 0;
    r.catnat_types = [...new Set((dCatnat?.data || []).slice(0,5).map(x => x.libDomCatNat || ""))].join(", ") || "—";
    r.catnat_derniere = dCatnat?.data?.[0]?.datFin || dCatnat?.data?.[0]?.dateDeb || "—";
    return r;
}

async function collecterFCU(lat, lon) {
    if (!lat || !lon) return null;
    const d = await apiFetch("https://france-chaleur-urbaine.beta.gouv.fr/api/v1/eligibility", { lat, lon });
    if (!d) return null;
    return {
        eligible:   d.isEligible ?? d.eligible ?? false,
        distance_m: d.distance ?? d.distanceToNetwork ?? "—",
        nom:        d.networkName ?? d.nom ?? "—",
        id:         d.networkId ?? d.identifiant_reseau ?? "—",
        enr_pct:    d.tauxENRR ?? "—",
        co2:        d.emissionCO2 ?? "—",
    };
}

async function collecterSIRENE(lat, lon, rayon = 0.2) {
    if (!lat || !lon) return [];
    const d = await apiFetch("https://recherche-entreprises.api.gouv.fr/search", { lat, long: lon, radius: rayon, per_page: 10 });
    return (d?.results || []).slice(0, 10).map(e => ({
        siret:    e.siret || "—",
        nom:      e.nom_complet || e.nom_raison_sociale || "—",
        naf_code: e.activite_principale || "—",
        naf_lib:  e.libelle_activite_principale || "—",
        effectif: e.tranche_effectif_salarie || "—",
        statut:   e.etat_administratif || "—",
    }));
}

async function collecterEducation(lat, lon, rayon = 500) {
    if (!lat || !lon) return [];
    // The dataset uses 'latitude'/'longitude' fields and 'position' geopoint
    // within_distance returns 400 — use bounding box approach instead
    const latDelta = rayon / 111000;  // ~1 deg lat = 111km
    const lonDelta = rayon / (111000 * Math.cos(lat * Math.PI / 180));
    const d = await apiFetch("https://data.education.gouv.fr/api/explore/v2.1/catalog/datasets/fr-en-annuaire-education/records", {
        where: `latitude >= ${lat - latDelta} AND latitude <= ${lat + latDelta} AND longitude >= ${lon - lonDelta} AND longitude <= ${lon + lonDelta} AND etat = 'OUVERT'`,
        limit: 10,
        select: "identifiant_de_l_etablissement,nom_etablissement,type_etablissement,libelle_nature,adresse_1,code_postal,nom_commune,telephone,mail,statut_public_prive,latitude,longitude,siren_siret,apprentissage,hebergement,restauration"
    });
    return d?.results || [];
}

async function collecterDVF(lat, lon, rayon = 150) {
    if (!lat || !lon) return [];
    const d = await apiFetch("https://apicarto.ign.fr/api/dvf/mutation", { lon, lat, dist: rayon, limit: 5 });
    return (d?.features || []).slice(0, 5).map(f => f.properties || {});
}

async function collecterBenchmark(codeCommune, libelleCommune, hint) {
    const r = {};
    // Try by commune name first (more reliable), then by INSEE code
    const queries = [libelleCommune, codeCommune].filter(Boolean);
    for (const q of queries) {
        const d = await apiFetch("https://data.ademe.fr/data-fair/api/v1/datasets/dpe-conso-tertiaire-par-commune/lines", { q, size: 3 });
        if (d?.results?.length) { r.commune = d.results[0]; break; }
    }
    if (hint) {
        const d = await apiFetch("https://data.ademe.fr/data-fair/api/v1/datasets/dpe-conso-tertiaire-par-activite/lines", { q: hint, size: 5 });
        if (d?.results?.length) r.activite = d.results;
    }
    return r;
}

async function fetchBuildingData(rnbId, type, adresseLabel, cleBan) {
    const res = await resoudreEntree(rnbId);
    const batId = res.bat_id_bdnb;
    if (!batId) return { res, error: "BDNB missing" };
    
    const p1 = { batiment_groupe_id: `eq.${batId}`, limit: 1 };
    const pm = { batiment_groupe_id: `eq.${batId}`, order: "millesime.desc", limit: 4 };

    const [dBase, dUsage, dFfo, dTopo, dProp, dRisque, dReseau, dDpeT, dDpeP, dElec, dGaz, dSitadel] = await Promise.all([
        bdnbQuery("batiment_groupe", { ...p1, select: "code_commune_insee,libelle_commune_insee,code_iris,s_geom_groupe" }),
        bdnbQuery("batiment_groupe_synthese_propriete_usage", { ...p1, select: "usage_principal_bdnb_open,categorie_usage_propriete" }),
        bdnbQuery("batiment_groupe_ffo_bat", { ...p1, select: "annee_construction,mat_mur_txt,mat_toit_txt,usage_niveau_1_txt,nb_niveau,nb_log" }),
        bdnbQuery("batiment_groupe_bdtopo_bat", { ...p1, select: "hauteur_mean,altitude_sol_mean,l_usage_1,nb_etages" }),
        bdnbQuery("batiment_groupe_proprietaire", { ...p1, select: "bat_prop_denomination_proprietaire,bat_prop_type_proprietaire" }),
        bdnbQuery("batiment_groupe_risques", { ...p1, select: "alea_argile,alea_radon,alea_sismique" }),
        bdnbQuery("batiment_groupe_indicateur_reseau_chaud_froid", { ...p1, select: "indicateur_distance_au_reseau,reseau_en_construction,identifiant_reseau" }),
        bdnbQuery("batiment_groupe_dpe_tertiaire", { ...p1, select: "identifiant_dpe,classe_conso_energie_dpe_tertiaire,classe_emission_ges_dpe_tertiaire,conso_dpe_tertiaire_ep_m2,emission_ges_dpe_tertiaire_m2,type_energie_chauffage,date_etablissement_dpe,surface_utile,shon" }),
        bdnbQuery("batiment_groupe_dpe_representatif_logement", { ...p1, select: "identifiant_dpe,classe_bilan_dpe,classe_emission_ges,conso_5_usages_ep_m2,emission_ges_5_usages_m2,type_energie_chauffage,date_etablissement_dpe,surface_habitable_immeuble" }),
        bdnbQuery("batiment_groupe_dle_elec_multimillesime", { ...pm, select: "millesime,nb_pdl_tot,conso_tot" }),
        bdnbQuery("batiment_groupe_dle_gaz_multimillesime", { ...pm, select: "millesime,nb_pdl_tot,conso_tot" }),
        bdnbQuery("sitadel", { ...p1, limit: 5, select: "date_autorisation,type_autorisation,libelle_destination_principale" }),
    ]);

    const base = first(dBase), usage = first(dUsage), ffo = first(dFfo),
          topo = first(dTopo), prop = first(dProp), risque = first(dRisque),
          reseau = first(dReseau);

    // Dynamic resolution of the active DPE
    let dpe = null, bType = type;
    const hasT = dDpeT && dDpeT.length > 0;
    const hasP = dDpeP && dDpeP.length > 0;

    if (hasT && hasP) {
        dpe = type === "T" ? dDpeT[0] : dDpeP[0];
        bType = type;
    } else if (hasT) {
        dpe = dDpeT[0]; bType = "T";
    } else if (hasP) {
        dpe = dDpeP[0]; bType = "P";
    }

    const elecs = dElec?.length ? dElec : [{millesime: "2022"}];
    const gazs  = dGaz?.length  ? dGaz  : [{millesime: "2022"}];
    const elec = elecs[0], gaz = gazs[0];

    const lat = res.lat, lon = res.lon;
    const codeInsee = res.code_commune_insee || base?.code_commune_insee;

    // Call all new APIs in parallel
    const libelleCommune = base?.libelle_commune_insee;
    const [ademeRes, dGeo, dFcu, dSirene, dEdu, dDvf, dBench] = await Promise.all([
        collecterADEME(rnbId, res.adresse_label || adresseLabel, res.cle_ban || cleBan, bType, val(dpe?.identifiant_dpe)),
        collecterGeoriques(lat, lon, codeInsee),
        collecterFCU(lat, lon),
        collecterSIRENE(lat, lon),
        collecterEducation(lat, lon, 500),
        collecterDVF(lat, lon, 300),
        collecterBenchmark(codeInsee, libelleCommune, null),
    ]);

    return {
        res, base, usage, ffo, topo, prop, risque, reseau, dpe, elec, gaz, elecs, gazs,
        ademe: ademeRes.resultats, bType,
        sitadel: dSitadel || [],
        geo: dGeo || {},
        fcu: dFcu || null,
        sirene: dSirene || [],
        education: dEdu || [],
        dvf: dDvf || [],
        benchmark: dBench || {},
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

    // Risks (BDNB + Géorisques enrichi)
    const geo = dataCtx.geo || {};
    clearAndAppendRows("data-risks", [
        ["Clay / RGA (BDNB)",       val(risque?.alea_argile)],
        ["Clay Hazard (Géorisques)",val(geo.argile_alea)],
        ["Radon (BDNB)",            val(risque?.alea_radon)],
        ["Radon Class (Géorisques)",geo.radon_classe ? `Class ${geo.radon_classe}` : "—"],
        ["Seismic Zone",            geo.sismique_zone ? `Zone ${geo.sismique_zone} — ${geo.sismique_lib}` : val(risque?.alea_sismique)],
        ["Flood Zones (AZI)",       geo.inondation_nb !== undefined ? `${geo.inondation_nb} zone(s) — ${geo.inondation_detail}` : "—"],
        ["Underground Cavities (500m)", geo.cavites_nb !== undefined ? `${geo.cavites_nb}${geo.cavites_nb > 0 ? ` — ${geo.cavites_types}` : ""}` : "—"],
        ["Natural Disasters (CatNat)",  geo.catnat_nb !== undefined ? `${geo.catnat_nb} event(s) — ${geo.catnat_types || "—"}` : "—"],
        ["ICPE Facilities (500m)",      geo.icpe_nb !== undefined ? `${geo.icpe_nb}${geo.icpe_nb > 0 ? ` — ${geo.icpe_noms}` : ""}` : "—"],
    ]);

    // France Chaleur Urbaine (enrichi)
    const fcu = dataCtx.fcu;
    const fcuSection = $("section-fcu"), fcuGrid = $("data-fcu");
    if (fcu) {
        fcuSection.style.display = "";
        clearAndAppendRows("data-fcu", [
            ["Eligible for Connection", fcu.eligible ? "✓ YES" : "✗ No"],
            ["Distance to Network",     `${fcu.distance_m} m`],
            ["Network Name",            val(fcu.nom)],
            ["Network ID",              val(fcu.id)],
            ["Renewable Energy %",      fcu.enr_pct !== "—" ? `${fcu.enr_pct}%` : "—"],
            ["CO₂ Emission",            fcu.co2 !== "—" ? `${fcu.co2} kg/MWh` : "—"],
            ...(reseau?.indicateur_distance_au_reseau ? [["BDNB Distance", val(reseau.indicateur_distance_au_reseau)]] : []),
            ...(reseau?.reseau_en_construction !== undefined ? [["Under Construction", val(reseau.reseau_en_construction)]] : []),
        ]);
    } else {
        // Fallback: show BDNB network data only if any
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
            card.innerHTML = `<div class="ademe-header"><span class="ademe-numdpe">${e.siret}</span><span class="ademe-addr">${e.nom}</span></div><div class="ademe-detail">NAF: ${e.naf_code} — ${e.naf_lib} · Staff: ${e.effectif} · Status: ${e.statut}</div>`;
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
