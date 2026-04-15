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
        bdnbQuery("batiment_groupe_bdtopo_bat", { ...p1, select: "hauteur_mean,altitude_sol_mean,l_usage_1,l_etat,max_hauteur" }),
        bdnbQuery("rel_batiment_groupe_proprietaire_siren_open", { ...p1, select: "bat_prop_denomination_proprietaire,siren,nb_locaux_open,is_bailleur" }),
        bdnbQuery("batiment_groupe_risques", { ...p1, select: "alea_argile,alea_radon,alea_sismique" }),
        bdnbQuery("batiment_groupe_indicateur_reseau_chaud_froid", { ...p1, select: "indicateur_distance_au_reseau,reseau_en_construction,id_reseau" }),
        bdnbQuery("batiment_groupe_dpe_tertiaire", { ...p1, select: "identifiant_dpe,classe_conso_energie_dpe_tertiaire,classe_emission_ges_dpe_tertiaire,conso_dpe_tertiaire_ep_m2,emission_ges_dpe_tertiaire_m2,type_energie_chauffage,date_etablissement_dpe,surface_utile,shon" }),
        bdnbQuery("batiment_groupe_dpe_representatif_logement", { ...p1, select: "identifiant_dpe,classe_bilan_dpe,classe_emission_ges,conso_5_usages_ep_m2,emission_ges_5_usages_m2,type_energie_chauffage,date_etablissement_dpe,surface_habitable_immeuble" }),
        bdnbQuery("batiment_groupe_dle_elec_multimillesime", { ...pm, select: "millesime,nb_pdl_tot,conso_tot" }),
        bdnbQuery("batiment_groupe_dle_gaz_multimillesime", { ...pm, select: "millesime,nb_pdl_tot,conso_tot" }),
        bdnbQuery("sitadel", { ...p1, limit: 5, select: "date_reelle_autorisation,nature_projet,destination_principale,etat_avancement_projet,s_loc_creee,s_loc_demolie" }),
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

// ─────────────────────────────────────────────────────────────────────────────
// REGULATORY ALERTS ENGINE
// ─────────────────────────────────────────────────────────────────────────────
function computeAlerts(ctx) {
    const alerts = [];
    const { dpe, ffo, base, bType, elec, gaz, elecs, gazs, fcu } = ctx;
    const surface = dpe?.surface_utile || dpe?.shon || base?.s_geom_groupe || 0;
    const isT = bType === "T" || (ffo?.usage_niveau_1_txt || "").toLowerCase().includes("tertiaire");

    // -- 1. DECRET TERTIAIRE --------------------------------------------------
    if (isT && surface >= 1000) {
        const c = dpe?.conso_dpe_tertiaire_ep_m2;
        alerts.push({
            level: "urgent", icon: "\u2696\ufe0f", title: "D\u00e9cret Tertiaire \u2014 Assujetti",
            desc: "Surface " + Math.round(surface) + " m\u00b2 \u2265 1\u202f000\u202fm\u00b2 tertiaire. Obligations Operat\u00ae : \u221240\u202f% en 2030 \u00b7 \u221250\u202f% en 2040 \u00b7 \u221260\u202f% en 2050." + (c ? " Conso actuelle\u00a0: " + Math.round(c) + " kWh\u202fEP/m\u00b2/an." : ""),
            deadline: "Prochaine \u00e9ch\u00e9ance : 31/12/2030 (\u221240\u202f%)"
        });
    } else if (isT && surface >= 500) {
        alerts.push({
            level: "info", icon: "\u2139\ufe0f", title: "D\u00e9cret Tertiaire \u2014 Seuil Non Atteint",
            desc: "Surface " + Math.round(surface) + " m\u00b2 (< 1\u202f000\u202fm\u00b2). Non assujetti actuellement. Toute extension future peut d\u00e9clencher l\u2019obligation.",
            deadline: null
        });
    }

    // -- 2. LOI CLIMAT DPE F/G -----------------------------------------------
    const cls = (dpe?.classe_conso_energie_dpe_tertiaire || dpe?.classe_bilan_dpe || "").trim().toUpperCase();
    if (cls === "G") {
        alerts.push({ level: "urgent", icon: "\uD83D\uDEA8", title: "Loi Climat \u2014 DPE G\u00a0: Location Interdite",
            desc: "DPE\u202fG interdit \u00e0 la location depuis le 1er janvier 2025 (Loi Climat & R\u00e9silience). Mise en conformit\u00e9 obligatoire avant toute nouvelle location.",
            deadline: "Interdit depuis : Jan 2025" });
    } else if (cls === "F") {
        alerts.push({ level: "warning", icon: "\u26a0\ufe0f", title: "Loi Climat \u2014 DPE F\u00a0: Interdiction 2028",
            desc: "Les logements class\u00e9s F seront interdits \u00e0 la location d\u00e8s janvier 2028. Anticipez les travaux pour maintenir la valeur locative.",
            deadline: "Interdiction : Jan 2028 (\u223c3 ans)" });
    } else if (cls === "E") {
        alerts.push({ level: "info", icon: "\uD83D\uDCCB", title: "Loi Climat \u2014 DPE E\u00a0: \u00c9ch\u00e9ance 2034",
            desc: "Les logements class\u00e9s E seront interdits \u00e0 la location en 2034. \u00c9chelonnez les r\u00e9novations d\u00e8s maintenant.",
            deadline: "Interdiction pr\u00e9vue : Jan 2034" });
    } else if (["A", "B", "C", "D"].includes(cls)) {
        alerts.push({ level: "compliant", icon: "\u2705", title: "Loi Climat \u2014 DPE " + cls + "\u00a0: Conforme",
            desc: "Classe " + cls + " \u2014 aucune restriction de location. Conforme \u00e0 la Loi Climat & R\u00e9silience.",
            deadline: null });
    }

    // -- 3. PPE GAZ -----------------------------------------------------------
    const gazC = gazs?.[0]?.conso_tot || gaz?.conso_tot || 0;
    const hasGaz = gazC > 0 || (dpe?.type_energie_chauffage || "").toLowerCase().includes("gaz");
    const fcuOk = fcu?.fcu_eligible;
    const fcuD = fcu?.fcu_distance_m;
    if (hasGaz) {
        let trend = null;
        if (gazs?.length >= 2) {
            const o = gazs[gazs.length - 1]?.conso_tot || 0;
            const n = gazs[0]?.conso_tot || 0;
            if (o > 0) trend = Math.round((n - o) / o * 100);
        }
        const tm = trend !== null ? (trend < 0 ? " Tendance\u00a0: " + trend + "% vs " + (gazs[gazs.length-1]?.millesime || "r\u00e9f.") + "." : " \u26a0 Hausse +\u202f" + trend + "%.") : "";
        const fm = fcuOk ? " \u2726 R\u00e9seau chaleur \u00e0 " + fcuD + "m \u2014 alternative d\u00e9carbon\u00e9e." : "";
        alerts.push({
            level: fcuOk ? "warning" : "info", icon: "\uD83D\uDD25", title: "PPE \u2014 D\u00e9pendance Gaz Fossile",
            desc: (gazC / 1000).toFixed(0) + " MWh/an de gaz fossile." + tm + " La PPE 2024-2033 pr\u00e9voit la sortie progressive du gaz tertiaire. Aides CEE et MaPrimeRénov mobilisables." + fm,
            deadline: "Neutralit\u00e9 carbone cible : 2050 (REPowerEU)"
        });
    } else {
        alerts.push({ level: "compliant", icon: "\u26a1", title: "PPE \u2014 Z\u00e9ro Gaz Fossile",
            desc: "Aucune consommation de gaz fossile d\u00e9tect\u00e9e. Align\u00e9 avec les objectifs PPE.",
            deadline: null });
    }

    // -- 4. FCU OPPORTUNITE ---------------------------------------------------
    if (fcuOk && Number(fcuD) <= 50) {
        alerts.push({ level: "info", icon: "\uD83C\uDF3F", title: "Opportunit\u00e9 \u2014 R\u00e9seau Chaleur Imm\u00e9diat",
            desc: "R\u00e9seau de chaleur \u00e0 " + fcuD + "m. Raccordement possible imm\u00e9diatement pour d\u00e9carboner le chauffage et am\u00e9liorer le DPE.",
            deadline: "Action possible : d\u00e8s maintenant" });
    }

    return alerts;
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
