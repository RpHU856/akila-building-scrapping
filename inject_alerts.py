
import re

alert_code = r"""
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

"""

with open("builder.js", encoding="utf-8") as f:
    content = f.read()

MARKER = "        benchmark: dBench || {},\n    };\n}\n\nasync function runMultiScan()"

if "computeAlerts" in content:
    print("computeAlerts already present — skipping")
elif MARKER in content:
    new_content = content.replace(
        MARKER,
        "        benchmark: dBench || {},\n    };\n}\n" + alert_code + "\nasync function runMultiScan()"
    )
    with open("builder.js", "w", encoding="utf-8") as f:
        f.write(new_content)
    print("OK — computeAlerts injected")
else:
    print("MARKER NOT FOUND — check builder.js")
