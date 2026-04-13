/* ═══════════════════════════════════════════════════════════════════════
   AKILA PROSPECT — Application Logic
   Client-side reimplementation of the Python building scanner
   Sources: RNB · BAN · BDNB · ADEME · DPE Observatory
   ═══════════════════════════════════════════════════════════════════════ */

// ── API URLS ────────────────────────────────────────────────────────────
const API = {
    RNB_BASE:       "https://rnb-api.beta.gouv.fr/api/alpha/buildings",
    BAN_URL:        "https://api-adresse.data.gouv.fr/search/",
    BDNB_URL:       "https://api.bdnb.io/v1/bdnb/donnees",
    ADEME_TERT_OLD: "https://data.ademe.fr/data-fair/api/v1/datasets/dpe-tertiaire/lines",
    ADEME_TERT_NEW: "https://data.ademe.fr/data-fair/api/v1/datasets/dpe01tertiaire/lines",
    ADEME_LOG_OLD:  "https://data.ademe.fr/data-fair/api/v1/datasets/dpe-france/lines",
    ADEME_LOG_EXIST:"https://data.ademe.fr/data-fair/api/v1/datasets/dpe03existant/lines",
    ADEME_LOG_NEUF: "https://data.ademe.fr/data-fair/api/v1/datasets/dpe02neuf/lines",
};

// ── DEFAULT PYTHON SCRIPT (stored for editor) ──────────────────────────
const DEFAULT_SCRIPT = `"""
╔══════════════════════════════════════════════════════════════════════╗
║  AKILA_prospect.py — Scanner Hybride (ID Card + Rayon X)            ║
║  Entrée  : RNB / Clé BAN / Adresse libre                           ║
║  Sortie  : Terminal structuré + JSON automatique                    ║
║  Sources : RNB · BAN · BDNB · ADEME · Observatoire DPE             ║
║  Prérequis : pip install requests                                   ║
╚══════════════════════════════════════════════════════════════════════╝
"""

import requests
import json
import sys
import ast
import os

# ── URLS ──────────────────────────────────────────────────────────────────────
RNB_BASE   = "https://rnb-api.beta.gouv.fr/api/alpha/buildings"
BAN_URL    = "https://api-adresse.data.gouv.fr/search/"
BDNB_URL   = "https://api.bdnb.io/v1/bdnb/donnees"

# Bases ADEME Open Data — identifiants vérifiés sur data.ademe.fr
ADEME_TERT_OLD = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe-tertiaire/lines"    # tertiaire avant 2021
ADEME_TERT_NEW = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe01tertiaire/lines"   # tertiaire depuis 2021
ADEME_LOG_OLD  = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe-france/lines"       # logements avant 2021
ADEME_LOG_EXIST= "https://data.ademe.fr/data-fair/api/v1/datasets/dpe03existant/lines"    # logements existants depuis 2021 (CORRIGÉ)
ADEME_LOG_NEUF = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe02neuf/lines"        # logements neufs depuis 2021 (CORRIGÉ)

HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
SEP  = "─" * 70
SEP2 = "═" * 70

# ─────────────────────────────────────────────────────────────────────────────
# UTILITAIRES & RÉSOLUTION
# ─────────────────────────────────────────────────────────────────────────────
def get(url, params=None):
    try:
        r = requests.get(url, params=params or {}, headers=HEADERS, timeout=30)
        return r.json() if r.ok else None
    except Exception: return None

def bdnb(endpoint, params):
    return get(f"{BDNB_URL}/{endpoint}", params) or []

def detecter_type_entree(entree):
    e = entree.strip().upper()
    # Format RNB avec tirets : XXXX-XXXX-XXXX (3 blocs de 4 alphanum)
    if len(e) == 14 and e.count("-") == 2:
        blocs = e.split("-")
        if all(len(b) == 4 and b.isalnum() for b in blocs):
            return "rnb"
    # Format RNB sans tirets : XXXXXXXXXXXX (12 alphanum)
    if len(e) == 12 and e.isalnum() and not e.isdigit():
        return "rnb"
    # Clé BAN : CCCCC_VVVV_NNNNN (3 segments tous numériques)
    if e.count("_") == 2 and all(p.isdigit() for p in e.split("_")):
        return "ban"
    return "adresse"

def v(val, unit=""):
    if val in (None, "", "None", r"\\N", "\\\\N", "nan", "NaN", [], {}): 
        return "—"
    if isinstance(val, str) and val.startswith("[") and val.endswith("]"):
        try:
            val_list = ast.literal_eval(val)
            val = ", ".join(str(x) for x in val_list)
        except: pass
    elif isinstance(val, list):
        val = ", ".join(str(x) for x in val)
    s = str(val).strip()
    return f"{s} {unit}".strip() if unit and s != "—" else s

def resoudre_entree(entree):
    type_entree = detecter_type_entree(entree)
    res = {"entree_originale": entree, "type_entree": type_entree, "rnb_id": None, "cle_ban": None, "adresse_label": None, "bdnb_bat_construction_id": None, "bat_id_bdnb": None, "lat": None, "lon": None}

    def extraire_rnb(data):
        pt = data.get("point", {}).get("coordinates", [])
        if pt: res["lon"], res["lat"] = pt[0], pt[1]
        addrs = data.get("addresses", [])
        if addrs:
            a = addrs[0]
            res["adresse_label"] = f"{a.get('street_number','')} {a.get('street','')} {a.get('city_zipcode','')} {a.get('city_name','')}".strip()
            res["cle_ban"] = a.get("id")
        for ext in data.get("ext_ids", []):
            if ext.get("source") == "bdnb": res["bdnb_bat_construction_id"] = ext.get("id")

    if type_entree == "rnb":
        rnb_propre = f"{entree[:4]}-{entree[4:8]}-{entree[8:]}" if len(entree) == 12 else entree
        data = get(f"{RNB_BASE}/{rnb_propre}/", {"with_plots": 1})
        if not data: data = get(f"{RNB_BASE}/{entree}/", {"with_plots": 1})
        if data:
            res["rnb_id"] = data.get("rnb_id") or rnb_propre
            extraire_rnb(data)

    elif type_entree == "ban":
        res["cle_ban"] = entree
        data = get(f"{RNB_BASE}/address/", {"cle_interop": entree, "limit": 1})
        if data and data.get("results"):
            res["rnb_id"] = data["results"][0].get("rnb_id")
            extraire_rnb(data["results"][0])
        if not res["adresse_label"]:
            ban = get(BAN_URL, {"q": entree.replace("_", " "), "limit": 1, "type": "housenumber"})
            if not ban or not ban.get("features"):
                ban = get(BAN_URL, {"q": entree.replace("_", " "), "limit": 1})
            if ban and ban.get("features"):
                f = ban["features"][0]
                coords = f["geometry"]["coordinates"]
                res["lat"], res["lon"] = coords[1], coords[0]
                res["adresse_label"] = f["properties"].get("label")

    else:
        ban = get(BAN_URL, {"q": entree, "limit": 1, "type": "housenumber"}) or get(BAN_URL, {"q": entree, "limit": 1})
        if ban and ban.get("features"):
            p = ban["features"][0]["properties"]
            coords = ban["features"][0]["geometry"]["coordinates"]
            res["lat"], res["lon"] = coords[1], coords[0]
            res["cle_ban"], res["adresse_label"] = p.get("id"), p.get("label")
            rnb_data = get(f"{RNB_BASE}/address/", {"cle_interop": res["cle_ban"], "limit": 1, "min_score": 0.5})
            if rnb_data and rnb_data.get("results"):
                res["rnb_id"] = rnb_data["results"][0].get("rnb_id")
                extraire_rnb(rnb_data["results"][0])

    if res["bdnb_bat_construction_id"]:
        link = bdnb("batiment_construction", {"batiment_construction_id": f"eq.{res['bdnb_bat_construction_id']}", "select": "batiment_groupe_id", "limit": 1})
        if link: res["bat_id_bdnb"] = link[0].get("batiment_groupe_id")

    if not res["bat_id_bdnb"] and res["cle_ban"]:
        for table, champ in [("batiment_groupe_adresse", "cle_interop_adr_principale_ban"), ("rel_batiment_groupe_adresse", "cle_interop_adr")]:
            rows = bdnb(table, {champ: f"eq.{res['cle_ban']}", "select": "batiment_groupe_id", "limit": 1})
            if rows: res["bat_id_bdnb"] = rows[0].get("batiment_groupe_id"); break

    return res

# ─────────────────────────────────────────────────────────────────────────────
# REQUÊTE DIRECTE ADEME OPEN DATA
# ─────────────────────────────────────────────────────────────────────────────
def collecter_ademe_direct(rnb_id, adresse_label, cle_ban, type_cible, numero_dpe_connu=None):
    resultats = []
    erreurs = set()

    urls_tert = [ADEME_TERT_NEW, ADEME_TERT_OLD]
    urls_log  = [ADEME_LOG_EXIST, ADEME_LOG_NEUF, ADEME_LOG_OLD]
    urls_toutes = urls_tert + urls_log if type_cible == "T" else urls_log + urls_tert
    urls_ademe = urls_tert if type_cible == "T" else urls_log

    def requeter_ademe(url, params):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=30)
            if r.ok:
                data = r.json()
                if data and data.get("total", 0) > 0:
                    resultats.extend(data["results"])
                    return True
            else:
                erreurs.add(r.status_code)
        except requests.exceptions.RequestException:
            erreurs.add("TIMEOUT/RÉSEAU")
        return False

    if numero_dpe_connu and numero_dpe_connu not in ("—", "None", None):
        for url in urls_toutes:
            if requeter_ademe(url, {"qs": f'numero_dpe:"{numero_dpe_connu}"', "size": 1}):
                break
            if not resultats:
                if requeter_ademe(url, {"qs": f'Numero_DPE:"{numero_dpe_connu}"', "size": 1}):
                    break
            if not resultats:
                if requeter_ademe(url, {"q": numero_dpe_connu, "size": 1}):
                    break

    if not resultats and adresse_label:
        cp = ""
        if cle_ban and "_" in cle_ban:
            cp = cle_ban.split("_")[0]
        else:
            for m in adresse_label.split():
                if m.isdigit() and len(m) == 5:
                    cp = m
                    break
        nom_rue = " ".join(w for w in adresse_label.split() if not w.isdigit() and len(w) > 2)[:40]

        for url in urls_ademe:
            params = {"size": 5}
            if cp:
                params["qs"] = f'code_postal:"{cp}"'
            if nom_rue:
                params["q"] = nom_rue
            requeter_ademe(url, params)

    vus, uniques = set(), []
    for r in resultats:
        k = r.get("numero_dpe", id(r))
        if k not in vus:
            vus.add(k)
            uniques.append(r)

    return uniques, erreurs

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    print(f"\\n{SEP2}\\n  AKILA PROSPECT — CARTE D'IDENTITÉ BÂTIMENT \\n{SEP2}")

    choix = input("  Cible : [T]ertiaire (Entreprise) ou [P]articulier (Logement) ? [T/p] : ").strip().upper()
    type_cible = "P" if choix == "P" else "T"

    entree = input("  Identifiant RNB, clé BAN ou adresse : ").strip()
    if not entree:
        return

    print(f"\\n  [1/3] Verrouillage de la cible...")
    res = resoudre_entree(entree)
    bat_id = res.get("bat_id_bdnb")

    if not bat_id:
        print("\\n  ✗ Bâtiment introuvable dans la base BDNB.")
        return

    print(f"  [2/3] Extraction des caractéristiques BDNB...")
    p1 = {"batiment_groupe_id": f"eq.{bat_id}", "limit": 1}
    pm = {"batiment_groupe_id": f"eq.{bat_id}", "order": "millesime.desc", "limit": 4}

    def first(lst): return lst[0] if lst else {}

    d_base   = first(bdnb("batiment_groupe", {**p1, "select": "code_commune_insee,libelle_commune_insee,s_geom_groupe,code_iris"}))
    d_usage  = first(bdnb("batiment_groupe_synthese_propriete_usage", {**p1, "select": "usage_principal_bdnb_open"}))
    d_ffo    = first(bdnb("batiment_groupe_ffo_bat", {**p1, "select": "annee_construction,mat_mur_txt,mat_toit_txt,nb_log,nb_niveau,usage_niveau_1_txt"}))
    d_topo   = first(bdnb("batiment_groupe_bdtopo_bat", {**p1, "select": "hauteur_mean,altitude_sol_mean,l_usage_1"}))
    d_prop   = first(bdnb("batiment_groupe_proprietaire", {**p1, "select": "bat_prop_denomination_proprietaire"}))
    d_risque = first(bdnb("batiment_groupe_risques", {**p1, "select": "alea_argile,alea_radon,alea_sismique"}))
    d_reseau = first(bdnb("batiment_groupe_indicateur_reseau_chaud_froid", {**p1, "select": "indicateur_distance_au_reseau,reseau_en_construction"}))

    if type_cible == "T":
        d_dpe = first(bdnb("batiment_groupe_dpe_tertiaire", {**p1, "select": "identifiant_dpe,classe_conso_energie_dpe_tertiaire,classe_emission_ges_dpe_tertiaire,conso_dpe_tertiaire_ep_m2,emission_ges_dpe_tertiaire_m2,type_energie_chauffage,date_etablissement_dpe,surface_utile,shon"}))
    else:
        d_dpe = first(bdnb("batiment_groupe_dpe_representatif_logement", {**p1, "select": "identifiant_dpe,classe_bilan_dpe,classe_emission_ges,conso_5_usages_ep_m2,emission_ges_5_usages_m2,type_energie_chauffage,date_etablissement_dpe,surface_habitable_immeuble"}))

    d_elec = bdnb("batiment_groupe_dle_elec_multimillesime", {**pm, "select": "millesime,conso_tot,nb_pdl_tot"})
    d_gaz  = bdnb("batiment_groupe_dle_gaz_multimillesime", {**pm, "select": "millesime,conso_tot,nb_pdl_tot"})

    numero_dpe = v(d_dpe.get("identifiant_dpe"))

    print(f"  [3/3] Vérification dans l'Open Data (ADEME)...")
    donnees_ademe, erreurs_ademe = collecter_ademe_direct(
        res.get("rnb_id"), res.get("adresse_label"),
        res.get("cle_ban"), type_cible, numero_dpe if numero_dpe != "—" else None
    )

    # … affichage terminal …

if __name__ == "__main__":
    try: main()
    except KeyboardInterrupt: sys.exit(0)
`;

// ── UTILITIES ───────────────────────────────────────────────────────────
function val(v, unit = "") {
    if (v === null || v === undefined || v === "" || v === "None" || v === "\\N" || v === "nan" || v === "NaN") return "—";
    if (Array.isArray(v)) {
        if (v.length === 0) return "—";
        v = v.join(", ");
    }
    if (typeof v === "string" && v.startsWith("[") && v.endsWith("]")) {
        try {
            const arr = JSON.parse(v.replace(/'/g, '"'));
            v = arr.join(", ");
        } catch(e) {}
    }
    const s = String(v).trim();
    if (s === "" || s === "—") return "—";
    return unit && s !== "—" ? `${s} ${unit}`.trim() : s;
}

async function fetchJSON(url, params = {}) {
    try {
        const qs = new URLSearchParams(params).toString();
        const fullUrl = qs ? `${url}?${qs}` : url;
        const r = await fetch(fullUrl, {
            headers: { "Accept": "application/json" },
            signal: AbortSignal.timeout(30000)
        });
        if (!r.ok) return null;
        return await r.json();
    } catch(e) {
        console.warn("fetchJSON error:", url, e);
        return null;
    }
}

async function bdnbQuery(endpoint, params) {
    return await fetchJSON(`${API.BDNB_URL}/${endpoint}`, params) || [];
}

function first(arr) {
    return (Array.isArray(arr) && arr.length > 0) ? arr[0] : {};
}

function detectType(entree) {
    const e = entree.trim().toUpperCase();
    if (e.length === 14 && (e.match(/-/g) || []).length === 2) {
        const blocs = e.split("-");
        if (blocs.every(b => b.length === 4 && /^[A-Z0-9]+$/.test(b))) return "rnb";
    }
    if (e.length === 12 && /^[A-Z0-9]+$/.test(e) && !/^\d+$/.test(e)) return "rnb";
    if ((e.match(/_/g) || []).length === 2 && e.split("_").every(p => /^\d+$/.test(p))) return "ban";
    return "adresse";
}

// ── RESOLUTION ──────────────────────────────────────────────────────────
async function resoudreEntree(entree) {
    const type = detectType(entree);
    const res = {
        entree_originale: entree, type_entree: type,
        rnb_id: null, cle_ban: null, adresse_label: null,
        bdnb_bat_construction_id: null, bat_id_bdnb: null,
        lat: null, lon: null
    };

    function extraireRNB(data) {
        const pt = data?.point?.coordinates;
        if (pt && pt.length >= 2) { res.lon = pt[0]; res.lat = pt[1]; }
        const addrs = data?.addresses || [];
        if (addrs.length) {
            const a = addrs[0];
            res.adresse_label = [a.street_number, a.street, a.city_zipcode, a.city_name].filter(Boolean).join(" ").trim();
            res.cle_ban = a.id;
        }
        for (const ext of (data?.ext_ids || [])) {
            if (ext.source === "bdnb") res.bdnb_bat_construction_id = ext.id;
        }
    }

    if (type === "rnb") {
        let rnbClean = entree;
        if (entree.length === 12) rnbClean = `${entree.slice(0,4)}-${entree.slice(4,8)}-${entree.slice(8)}`;
        let data = await fetchJSON(`${API.RNB_BASE}/${rnbClean}/`, { with_plots: 1 });
        if (!data) data = await fetchJSON(`${API.RNB_BASE}/${entree}/`, { with_plots: 1 });
        if (data) {
            res.rnb_id = data.rnb_id || rnbClean;
            extraireRNB(data);
        }
    } else if (type === "ban") {
        res.cle_ban = entree;
        let data = await fetchJSON(`${API.RNB_BASE}/address/`, { cle_interop: entree, limit: 1 });
        if (data?.results?.length) {
            res.rnb_id = data.results[0].rnb_id;
            extraireRNB(data.results[0]);
        }
        if (!res.adresse_label) {
            let ban = await fetchJSON(API.BAN_URL, { q: entree.replace(/_/g, " "), limit: 1, type: "housenumber" });
            if (!ban?.features?.length) ban = await fetchJSON(API.BAN_URL, { q: entree.replace(/_/g, " "), limit: 1 });
            if (ban?.features?.length) {
                const f = ban.features[0];
                res.lat = f.geometry.coordinates[1];
                res.lon = f.geometry.coordinates[0];
                res.adresse_label = f.properties?.label;
            }
        }
    } else {
        let ban = await fetchJSON(API.BAN_URL, { q: entree, limit: 1, type: "housenumber" });
        if (!ban?.features?.length) ban = await fetchJSON(API.BAN_URL, { q: entree, limit: 1 });
        if (ban?.features?.length) {
            const p = ban.features[0].properties;
            const coords = ban.features[0].geometry.coordinates;
            res.lat = coords[1]; res.lon = coords[0];
            res.cle_ban = p?.id; res.adresse_label = p?.label;
            const rnbData = await fetchJSON(`${API.RNB_BASE}/address/`, { cle_interop: res.cle_ban, limit: 1, min_score: 0.5 });
            if (rnbData?.results?.length) {
                res.rnb_id = rnbData.results[0].rnb_id;
                extraireRNB(rnbData.results[0]);
            }
        }
    }

    // Resolve bat_id_bdnb
    if (res.bdnb_bat_construction_id) {
        const link = await bdnbQuery("batiment_construction", {
            batiment_construction_id: `eq.${res.bdnb_bat_construction_id}`,
            select: "batiment_groupe_id", limit: 1
        });
        if (link.length) res.bat_id_bdnb = link[0].batiment_groupe_id;
    }
    if (!res.bat_id_bdnb && res.cle_ban) {
        const tables = [
            ["batiment_groupe_adresse", "cle_interop_adr_principale_ban"],
            ["rel_batiment_groupe_adresse", "cle_interop_adr"]
        ];
        for (const [table, champ] of tables) {
            const rows = await bdnbQuery(table, { [champ]: `eq.${res.cle_ban}`, select: "batiment_groupe_id", limit: 1 });
            if (rows.length) { res.bat_id_bdnb = rows[0].batiment_groupe_id; break; }
        }
    }

    return res;
}

// ── ADEME QUERIES ───────────────────────────────────────────────────────
async function collecterADEME(rnbId, adresseLabel, cleBan, typeCible, numeroDpeConnu) {
    let resultats = [];
    const erreurs = new Set();

    const urlsTert = [API.ADEME_TERT_NEW, API.ADEME_TERT_OLD];
    const urlsLog = [API.ADEME_LOG_EXIST, API.ADEME_LOG_NEUF, API.ADEME_LOG_OLD];
    const urlsToutes = typeCible === "T" ? [...urlsTert, ...urlsLog] : [...urlsLog, ...urlsTert];
    const urlsAdeme = typeCible === "T" ? urlsTert : urlsLog;

    async function requeterAdeme(url, params) {
        try {
            const r = await fetch(`${url}?${new URLSearchParams(params)}`, {
                headers: { "Accept": "application/json" },
                signal: AbortSignal.timeout(30000)
            });
            if (r.ok) {
                const data = await r.json();
                if (data && (data.total || 0) > 0 && data.results) {
                    resultats.push(...data.results);
                    return true;
                }
            } else {
                erreurs.add(r.status);
            }
        } catch(e) {
            erreurs.add("TIMEOUT");
        }
        return false;
    }

    // Priority 1: known DPE number
    if (numeroDpeConnu && numeroDpeConnu !== "—") {
        for (const url of urlsToutes) {
            if (await requeterAdeme(url, { qs: `numero_dpe:"${numeroDpeConnu}"`, size: 1 })) break;
            if (!resultats.length && await requeterAdeme(url, { qs: `Numero_DPE:"${numeroDpeConnu}"`, size: 1 })) break;
            if (!resultats.length && await requeterAdeme(url, { q: numeroDpeConnu, size: 1 })) break;
        }
    }

    // Priority 2: address search
    if (!resultats.length && adresseLabel) {
        let cp = "";
        if (cleBan && cleBan.includes("_")) {
            cp = cleBan.split("_")[0];
        } else {
            for (const m of adresseLabel.split(" ")) {
                if (/^\d{5}$/.test(m)) { cp = m; break; }
            }
        }
        const nomRue = adresseLabel.split(" ").filter(w => !/^\d+$/.test(w) && w.length > 2).join(" ").slice(0, 40);
        for (const url of urlsAdeme) {
            const params = { size: 5 };
            if (cp) params.qs = `code_postal:"${cp}"`;
            if (nomRue) params.q = nomRue;
            await requeterAdeme(url, params);
        }
    }

    // Deduplicate
    const seen = new Set();
    const uniques = [];
    for (const r of resultats) {
        const k = r.numero_dpe || r.Numero_DPE || Math.random();
        if (!seen.has(k)) { seen.add(k); uniques.push(r); }
    }

    return { resultats: uniques, erreurs: [...erreurs] };
}

// ── CHAMP helper (like Python version) ──────────────────────────────────
function champ(d, ...keys) {
    for (const k of keys) {
        const v = d[k];
        if (v !== null && v !== undefined && v !== "" && v !== "None" && v !== "nan") return String(v);
    }
    return "—";
}

// ── DOM HELPERS ─────────────────────────────────────────────────────────
const $ = id => document.getElementById(id);

function createDataRow(label, value) {
    const row = document.createElement("div");
    row.className = "data-row";
    const lbl = document.createElement("span");
    lbl.className = "data-label";
    lbl.textContent = label;
    const vl = document.createElement("span");
    vl.className = `data-value${value === "—" ? " dim" : ""}`;
    if (typeof value === "string" && value.startsWith("http")) {
        const a = document.createElement("a");
        a.href = value;
        a.target = "_blank";
        a.rel = "noopener";
        a.textContent = value;
        vl.appendChild(a);
    } else {
        vl.textContent = value;
    }
    row.appendChild(lbl);
    row.appendChild(vl);
    return row;
}

function clearAndAppendRows(containerId, rows) {
    const container = $(containerId);
    container.innerHTML = "";
    for (const [label, value] of rows) {
        container.appendChild(createDataRow(label, value));
    }
}

function showToast(message, type = "info") {
    const container = $("toast-container");
    const toast = document.createElement("div");
    toast.className = `toast ${type}`;
    toast.textContent = message;
    container.appendChild(toast);
    setTimeout(() => {
        toast.style.animation = `slideOutToast 300ms ease forwards`;
        setTimeout(() => toast.remove(), 300);
    }, 4000);
}

function getDPEColor(letter) {
    const colors = { A: "#319834", B: "#33CC31", C: "#CCFF33", D: "#E6E600", E: "#FFCC00", F: "#FF9A00", G: "#FF4444" };
    return colors[letter?.toUpperCase()] || "var(--text-muted)";
}

// ── STATE & UI ──────────────────────────────────────────────────────────
let currentType = "T";
let searchHistory = JSON.parse(localStorage.getItem("akila_history") || "[]");
let scanData = null;
let leafletMap = null;
let geoJsonLayer = null;
let selectedRnbIds = new Set();
let centerRnbId = null;

function setStatus(mode, text) {
    const indicator = $("status-indicator");
    indicator.className = `status-indicator ${mode}`;
    indicator.querySelector(".status-text").textContent = text;
}

function showState(stateId) {
    ["welcome-state", "loading-state", "error-state", "map-container", "results-container"].forEach(id => {
        $(id).style.display = id === stateId ? "" : "none";
    });
}

function setStep(n) {
    for (let i = 1; i <= 3; i++) {
        const el = $(`step-${i}`);
        el.className = i < n ? "step done" : i === n ? "step active" : "step";
    }
}

function addToHistory(entree, type) {
    searchHistory = searchHistory.filter(h => h.entree !== entree);
    searchHistory.unshift({ entree, type, ts: Date.now() });
    if (searchHistory.length > 15) searchHistory.pop();
    localStorage.setItem("akila_history", JSON.stringify(searchHistory));
    renderHistory();
}

function renderHistory() {
    const container = $("history-list");
    if (!searchHistory.length) {
        container.innerHTML = '<p class="empty-state">No searches yet</p>';
        return;
    }
    container.innerHTML = "";
    for (const h of searchHistory) {
        const item = document.createElement("div");
        item.className = "history-item";
        item.innerHTML = `
            <svg class="history-icon" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>
            <span class="history-text">${h.entree}</span>
            <span class="history-badge">${h.type === "T" ? "Comm." : "Resid."}</span>
        `;
        item.addEventListener("click", () => {
            $("input-entree").value = h.entree;
            currentType = h.type;
            updateToggle();
            runScan();
        });
        container.appendChild(item);
    }
}

function updateToggle() {
    $("btn-tertiaire").classList.toggle("active", currentType === "T");
    $("btn-particulier").classList.toggle("active", currentType === "P");
}

// ── MAIN SCAN ───────────────────────────────────────────────────────────
async function runScan() {
    const entree = $("input-entree").value.trim();
    if (!entree) { showToast("Please enter an ID or address.", "error"); return; }

    showState("loading-state");
    setStatus("loading", "Scanning Area...");
    $("btn-search").disabled = true;

    try {
        setStep(1);
        const res = await resoudreEntree(entree);
        // Wait, did we get coordinates?
        if (!res.lat || !res.lon) {
            showState("error-state");
            $("error-title").textContent = "Location not found";
            $("error-message").textContent = "Could not resolve geographic coordinates for this input.";
            setStatus("error", "Error");
            $("btn-search").disabled = false;
            return;
        }
        
        await renderMapSelection(res);
        addToHistory(entree, currentType);
    } catch (e) {
        showState("error-state");
        $("error-title").textContent = "Error";
        $("error-message").textContent = e.message;
        setStatus("error", "Error");
        $("btn-search").disabled = false;
    }
}

async function renderMapSelection(res) {
    showState("map-container");
    setStatus("ready", "Select buildings");
    $("btn-search").disabled = false;
    
    if (leafletMap) {
        leafletMap.remove();
        leafletMap = null;
    }
    
    leafletMap = L.map('map').setView([res.lat, res.lon], 18);
    L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
        attribution: '&copy; OpenStreetMap'
    }).addTo(leafletMap);
    
    L.circleMarker([res.lat, res.lon], {radius: 8, color: "var(--akila-green)", fillColor: "var(--akila-green)", fillOpacity: 1}).addTo(leafletMap).bindPopup("Target").openPopup();
    
    const offset = 0.002;
    const bbox = `${res.lon - offset},${res.lat - offset},${res.lon + offset},${res.lat + offset}`;
    
    selectedRnbIds.clear();
    if (res.rnb_id) selectedRnbIds.add(res.rnb_id);
    centerRnbId = res.rnb_id;
    updateMapActions();
    
    try {
        let apiUrl = `${API.RNB_BASE}/?bbox=${bbox}&with_plots=1&limit=100`;
        let allFeatures = [];
        
        while (apiUrl) {
            const buildingsData = await fetchJSON(apiUrl);
            if (!buildingsData || !buildingsData.results) break;
            
            const features = buildingsData.results.filter(r => r.shape).map(r => ({
                type: "Feature",
                geometry: r.shape,
                properties: { rnb_id: r.rnb_id, status: r.status }
            }));
            
            allFeatures.push(...features);
            apiUrl = buildingsData.next || null;
            if (allFeatures.length > 500) break; // Sanity limit for rendering
        }
        
        if (allFeatures.length > 0) {
            geoJsonLayer = L.geoJSON({type: "FeatureCollection", features: allFeatures}, {
                style: function(feature) {
                    const isSelected = selectedRnbIds.has(feature.properties.rnb_id);
                    return {
                        fillColor: isSelected ? "var(--akila-blue)" : "#484F58",
                        color: isSelected ? "#fff" : "#ccc",
                        weight: 2,
                        fillOpacity: isSelected ? 0.7 : 0.3
                    };
                },
                onEachFeature: function(feature, layer) {
                    layer.on('click', function() {
                        const id = feature.properties.rnb_id;
                        if (selectedRnbIds.has(id)) {
                            selectedRnbIds.delete(id);
                        } else {
                            selectedRnbIds.add(id);
                        }
                        geoJsonLayer.setStyle(function(f) {
                            const isSelected = selectedRnbIds.has(f.properties.rnb_id);
                            return {
                                fillColor: isSelected ? "var(--akila-blue)" : "#484F58",
                                color: isSelected ? "#fff" : "#ccc",
                                fillOpacity: isSelected ? 0.7 : 0.3
                            };
                        });
                        updateMapActions();
                    });
                    layer.bindTooltip(`RNB: ${feature.properties.rnb_id}`);
                }
            }).addTo(leafletMap);
        }
    } catch(e) {
        console.error("Map load error", e);
    }
}

function updateMapActions() {
    $("map-selection-info").textContent = `${selectedRnbIds.size} building(s) selected`;
    $("btn-run-multi-scan").style.display = selectedRnbIds.size > 0 ? "block" : "none";
}

// Attach listener
$("btn-run-multi-scan").addEventListener("click", runMultiScan);

async function runMultiScan() {
    if (selectedRnbIds.size === 0) return;
    
    showState("loading-state");
    setStatus("loading", "Processing Multi-Scan...");
    
    try {
        setStep(2);
        
        let mergedBdnb = {
            s_geom_groupe: 0,
            nb_log: 0,
            surface_utile: 0,
            surface_habitable_immeuble: 0,
            conso_elec: 0,
            conso_gaz: 0
        };
        
        const allAdeme = [];
        const rnbIds = Array.from(selectedRnbIds);
        let firstRes = null;
        let firstBatId = null;
        
        for (const rnbId of rnbIds) {
            const res = await resoudreEntree(rnbId);
            if (!firstRes) firstRes = res;
            
            const batId = res.bat_id_bdnb;
            if (!batId) continue;
            if (!firstBatId) firstBatId = batId;
            
            const p1 = { batiment_groupe_id: `eq.${batId}`, limit: 1 };
            const pm = { batiment_groupe_id: `eq.${batId}`, order: "millesime.desc", limit: 4 };

            const [dBase, dFfo, dDpe, dElec, dGaz] = await Promise.all([
                bdnbQuery("batiment_groupe", { ...p1, select: "s_geom_groupe" }),
                bdnbQuery("batiment_groupe_ffo_bat", { ...p1, select: "nb_log" }),
                currentType === "T"
                    ? bdnbQuery("batiment_groupe_dpe_tertiaire", { ...p1, select: "surface_utile" })
                    : bdnbQuery("batiment_groupe_dpe_representatif_logement", { ...p1, select: "surface_habitable_immeuble" }),
                bdnbQuery("batiment_groupe_dle_elec_multimillesime", { ...pm, select: "conso_tot" }),
                bdnbQuery("batiment_groupe_dle_gaz_multimillesime", { ...pm, select: "conso_tot" })
            ]);
            
            // Accumulate numeric data
            if (dBase[0]?.s_geom_groupe) mergedBdnb.s_geom_groupe += Number(dBase[0].s_geom_groupe);
            if (dFfo[0]?.nb_log) mergedBdnb.nb_log += Number(dFfo[0].nb_log);
            if (currentType === "T" && dDpe[0]?.surface_utile) mergedBdnb.surface_utile += Number(dDpe[0].surface_utile);
            if (currentType === "P" && dDpe[0]?.surface_habitable_immeuble) mergedBdnb.surface_habitable_immeuble += Number(dDpe[0].surface_habitable_immeuble);
            if (dElec[0]?.conso_tot) mergedBdnb.conso_elec += Number(dElec[0].conso_tot);
            if (dGaz[0]?.conso_tot) mergedBdnb.conso_gaz += Number(dGaz[0].conso_tot);
            
            // ADEME (Step 3 inside loop)
            const ademe = await collecterADEME(rnbId, res.adresse_label, res.cle_ban, currentType, null);
            if (ademe.resultats.length) allAdeme.push(...ademe.resultats);
        }

        if (!firstBatId) {
            showState("error-state");
            $("error-title").textContent = "BDNB Extraction Failed";
            $("error-message").textContent = "None of the selected buildings were found in BDNB.";
            setStatus("error", "Error");
            return;
        }

        setStep(3);
        
        // Fetch remaining structural/ownership metadata using the FIRST building to represent the complex
        const p1_first = { batiment_groupe_id: `eq.${firstBatId}`, limit: 1 };
        const pm_first = { batiment_groupe_id: `eq.${firstBatId}`, order: "millesime.desc", limit: 4 };

        const [dBase, dUsage, dFfo, dTopo, dProp, dRisque, dReseau, dDpe, dElec, dGaz] = await Promise.all([
            bdnbQuery("batiment_groupe", { ...p1_first, select: "code_commune_insee,libelle_commune_insee,code_iris" }),
            bdnbQuery("batiment_groupe_synthese_propriete_usage", { ...p1_first, select: "usage_principal_bdnb_open" }),
            bdnbQuery("batiment_groupe_ffo_bat", { ...p1_first, select: "annee_construction,mat_mur_txt,mat_toit_txt,usage_niveau_1_txt,nb_niveau" }),
            bdnbQuery("batiment_groupe_bdtopo_bat", { ...p1_first, select: "hauteur_mean,altitude_sol_mean,l_usage_1" }),
            bdnbQuery("batiment_groupe_proprietaire", { ...p1_first, select: "bat_prop_denomination_proprietaire" }),
            bdnbQuery("batiment_groupe_risques", { ...p1_first, select: "alea_argile,alea_radon,alea_sismique" }),
            bdnbQuery("batiment_groupe_indicateur_reseau_chaud_froid", { ...p1_first, select: "indicateur_distance_au_reseau,reseau_en_construction" }),
            currentType === "T"
                ? bdnbQuery("batiment_groupe_dpe_tertiaire", { ...p1_first, select: "identifiant_dpe,classe_conso_energie_dpe_tertiaire,classe_emission_ges_dpe_tertiaire,conso_dpe_tertiaire_ep_m2,emission_ges_dpe_tertiaire_m2,type_energie_chauffage,date_etablissement_dpe" })
                : bdnbQuery("batiment_groupe_dpe_representatif_logement", { ...p1_first, select: "identifiant_dpe,classe_bilan_dpe,classe_emission_ges,conso_5_usages_ep_m2,emission_ges_5_usages_m2,type_energie_chauffage,date_etablissement_dpe" }),
            bdnbQuery("batiment_groupe_dle_elec_multimillesime", { ...pm_first, select: "millesime,nb_pdl_tot" }),
            bdnbQuery("batiment_groupe_dle_gaz_multimillesime", { ...pm_first, select: "millesime,nb_pdl_tot" }),
        ]);

        const base = first(dBase), usage = first(dUsage), ffo = first(dFfo),
              topo = first(dTopo), prop = first(dProp), risque = first(dRisque),
              reseau = first(dReseau), dpe = first(dDpe);
              
        // Inject merged data back into representative objects so UI renders them
        base.s_geom_groupe = mergedBdnb.s_geom_groupe;
        ffo.nb_log = mergedBdnb.nb_log;
        
        let fakeElec = first(dElec) || {millesime: "2022"};
        fakeElec.conso_tot = mergedBdnb.conso_elec || null;
        let fakeGaz = first(dGaz) || {millesime: "2022"};
        fakeGaz.conso_tot = mergedBdnb.conso_gaz || null;

        if (currentType === "T") dpe.surface_utile = mergedBdnb.surface_utile;
        else dpe.surface_habitable_immeuble = mergedBdnb.surface_habitable_immeuble;
        const numeroDPE = val(dpe?.identifiant_dpe);

        // Store scan data for export
        scanData = {
            adresse: firstRes.adresse_label || "Multiple Buildings Selected",
            resolution: firstRes,
            bdnb: { base, usage, ffo, topo, dpe, risques: risque, reseau, elec: fakeElec, gaz: fakeGaz },
            ademe: allAdeme,
            genere_le: new Date().toISOString()
        };

        // ── RENDER RESULTS ──────────────────────────────────────────
        showState("results-container");

        // Banner
        const adresseAffichee = firstRes.adresse_label || "Multiple Buildings Selected";
        $("result-address").textContent = adresseAffichee;
        $("badge-type").textContent = currentType === "T" ? "Commercial" : "Residential";

        const etiquette = currentType === "T"
            ? val(dpe.classe_conso_energie_dpe_tertiaire)
            : val(dpe.classe_bilan_dpe);
        const dpeBadge = $("badge-dpe");
        dpeBadge.textContent = `DPE ${etiquette}`;
        dpeBadge.className = `badge badge-dpe${etiquette !== "—" ? ` dpe-${etiquette.toUpperCase()}` : ""}`;

        // IDs
        const rnbLink = firstRes.rnb_id ? `https://rnb.beta.gouv.fr/batiment/${firstRes.rnb_id}` : "—";
        const bdnbLink = `https://open.bdnb.io/?batiment_groupe_id=${firstBatId}`;
        clearAndAppendRows("data-ids", [
            ["RNB ID (Building)", val(firstRes.rnb_id)],
            ["BDNB ID (CSTB)", val(firstBatId)],
            ["BAN Key (Interop)", val(firstRes.cle_ban)],
            ...(firstRes.rnb_id ? [["RNB Record", rnbLink]] : []),
            ["BDNB Record", bdnbLink],
        ]);

        // Location
        clearAndAppendRows("data-location", [
            ["Municipality (INSEE)", `${val(base.code_commune_insee)} - ${val(base.libelle_commune_insee)}`],
            ["IRIS Code", val(base.code_iris)],
            ["GPS Coordinates", `${val(firstRes.lat)}, ${val(firstRes.lon)}`],
        ]);

        // Physical
        const surf = base.s_geom_groupe;
        const surfVal = surf ? `${Math.round(parseFloat(surf))} m²` : "—";
        let ageStr = "—";
        try {
            const annee = parseInt(ffo.annee_construction);
            if (!isNaN(annee)) ageStr = `${2025 - annee} years`;
        } catch(e) {}
        clearAndAppendRows("data-physical", [
            ["Year of Construction", val(ffo.annee_construction)],
            ["Building Age", ageStr],
            ["Ground Footprint", surfVal],
            ["Average Height", val(topo.hauteur_mean, "m")],
            ["Ground Altitude", val(topo.altitude_sol_mean, "m")],
            ["Number of Floors", val(ffo.nb_niveau)],
            ["Wall Materials", val(ffo.mat_mur_txt)],
            ["Roof Materials", val(ffo.mat_toit_txt)],
            ["BD TOPO Usages", val(topo.l_usage_1)],
        ]);

        // Usage & Property
        const propVal = val(prop.bat_prop_denomination_proprietaire || prop.l_denomination_proprietaire);
        clearAndAppendRows("data-usage", [
            ["Primary Use (BDNB)", val(usage.usage_principal_bdnb_open)],
            ["Land Use (FF)", val(ffo.usage_niveau_1_txt)],
            ["Owner", propVal],
        ]);

        // Energy Performance
        const energyLabels = $("energy-labels");
        energyLabels.innerHTML = "";

        const etiqGes = currentType === "T"
            ? val(dpe.classe_emission_ges_dpe_tertiaire)
            : val(dpe.classe_emission_ges);

        // Energy label card
        const eCard = document.createElement("div");
        eCard.className = "energy-label-card";
        eCard.innerHTML = `
            <div class="label-title">Energy</div>
            <div class="label-grade" style="color:${getDPEColor(etiquette)}">${etiquette}</div>
            <div class="label-value">${currentType === "T" ? val(dpe.conso_dpe_tertiaire_ep_m2, "kWh/m²/an") : val(dpe.conso_5_usages_ep_m2, "kWh/m²/an")}</div>
        `;
        energyLabels.appendChild(eCard);

        // GES label card
        const gCard = document.createElement("div");
        gCard.className = "energy-label-card";
        gCard.innerHTML = `
            <div class="label-title">GES</div>
            <div class="label-grade" style="color:${getDPEColor(etiqGes)}">${etiqGes}</div>
            <div class="label-value">${currentType === "T" ? val(dpe.emission_ges_dpe_tertiaire_m2, "kg CO₂/m²/an") : val(dpe.emission_ges_5_usages_m2, "kg CO₂/m²/an")}</div>
        `;
        energyLabels.appendChild(gCard);

        // Energy details
        const energyRows = [
            ["Linked DPE Number", numeroDPE],
            ["Heating Energy", val(dpe.type_energie_chauffage)],
            ["DPE Date", val(dpe.date_etablissement_dpe)],
        ];
        if (currentType === "T") {
            energyRows.push(["Usable Area", val(dpe.surface_utile, "m²")]);
        } else {
            energyRows.push(["Living Area", val(dpe.surface_habitable_immeuble, "m²")]);
        }
        clearAndAppendRows("data-energy", energyRows);

        // Consumption
        const consoContainer = $("data-consumption");
        consoContainer.innerHTML = "";

        function renderConsoSection(title, data, barClass, icon) {
            const section = document.createElement("div");
            section.className = "consumption-section";
            section.innerHTML = `<h4>${icon} ${title}</h4>`;

            if (!data || !data.length) {
                section.innerHTML += '<p style="color:var(--text-muted);font-size:0.82rem;padding-left:4px;">Not available in open data.</p>';
                consoContainer.appendChild(section);
                return;
            }

            const maxConso = Math.max(...data.map(d => parseFloat(d.conso_tot) || 0));

            for (const d of data) {
                const c = parseFloat(d.conso_tot) || 0;
                const pct = maxConso > 0 ? (c / maxConso * 100) : 0;
                const row = document.createElement("div");
                row.className = "conso-row";
                const cost = barClass === "elec" ? `~${Math.round(c * 0.15 / 1000).toLocaleString("en-US")} k€/yr` : "";
                row.innerHTML = `
                    <span class="conso-year">${d.millesime || "—"}</span>
                    <div class="conso-bar-bg"><div class="conso-bar ${barClass}" style="width:0%"></div></div>
                    <span class="conso-value">${Math.round(c).toLocaleString("en-US")} kWh</span>
                    <span class="conso-cost">${cost}</span>
                `;
                section.appendChild(row);
                // Animate bar
                requestAnimationFrame(() => {
                    requestAnimationFrame(() => {
                        row.querySelector(`.conso-bar`).style.width = `${pct}%`;
                    });
                });
            }

            // Trend
            if (data.length >= 2) {
                const vals = data.map(d => parseFloat(d.conso_tot) || 0);
                const moy = vals.reduce((a,b) => a+b, 0) / vals.length;
                const last = vals[vals.length - 1];
                const delta = last ? ((vals[0] - last) / last * 100) : 0;
                const arrow = delta < 0 ? "↓" : "↑";
                const trend = document.createElement("div");
                trend.className = "conso-trend";
                trend.textContent = `Trend: ${arrow} ${Math.abs(delta).toFixed(1)}%  |  Average: ${Math.round(moy).toLocaleString("en-US")} kWh/yr`;
                section.appendChild(trend);
            }

            consoContainer.appendChild(section);
        }

        renderConsoSection("Electricity", dElec, "elec", "⚡");
        renderConsoSection("Gas", dGaz, "gaz", "🔥");

        // Risks
        clearAndAppendRows("data-risks", [
            ["Clay / Shrink-Swell", val(risque.alea_argile)],
            ["Radon", val(risque.alea_radon)],
            ["Seismic", val(risque.alea_sismique)],
            ...(reseau.indicateur_distance_au_reseau ? [["Heat Network (distance)", val(reseau.indicateur_distance_au_reseau)]] : []),
            ...(reseau.reseau_en_construction !== undefined ? [["Network Under Construction", val(reseau.reseau_en_construction)]] : []),
        ]);

        // ADEME Direct
        const ademeSection = $("section-ademe");
        const ademeData = $("data-ademe");
        if (allAdeme.length) {
            ademeSection.style.display = "";
            ademeData.innerHTML = "";
            for (const d of allAdeme.slice(0, 3)) {
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
        if (numeroDPE !== "—") {
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
        const allData = { ...base, ...usage, ...ffo, ...topo, ...dpe, ...risque, ...reseau };
        const exclude = new Set(["geom_groupe", "geom_groupe_pos_wgs84", "geom_cstr", "geom_adresse"]);
        const sortedKeys = Object.keys(allData).filter(k => !exclude.has(k) && allData[k] !== null && allData[k] !== "" && allData[k] !== "None").sort();
        for (const key of sortedKeys) {
            const row = document.createElement("div");
            row.className = "raw-row";
            row.innerHTML = `<span class="raw-key">${key}</span><span class="raw-val">${val(allData[key])}</span>`;
            rawContainer.appendChild(row);
        }

        // Success
        setStatus("", "Scan complete");
        addToHistory(entree, currentType);
        showToast(`Scan complete: ${adresseAffichee}`, "success");

    } catch(e) {
        console.error("Scan error:", e);
        showState("error-state");
        $("error-title").textContent = "Unexpected Error";
        $("error-message").textContent = e.message || "An unexpected error occurred.";
        setStatus("error", "Error");
    }

    $("btn-search").disabled = false;
}

// ── EXPORT JSON ─────────────────────────────────────────────────────────
function exportJSON() {
    if (!scanData) { showToast("No scan data to export.", "error"); return; }
    const blob = new Blob([JSON.stringify(scanData, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    const slug = scanData.adresse.toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g, "").replace(/[^a-z0-9]+/g, "_").slice(0, 40);
    const ts = new Date().toISOString().replace(/[-:T]/g, "").slice(0, 13);
    a.href = url;
    a.download = `akila_${slug}_${ts}.json`;
    a.click();
    URL.revokeObjectURL(url);
    showToast("JSON exported!", "success");
}

function copyResults() {
    if (!scanData) { showToast("No scan data to copy.", "error"); return; }
    navigator.clipboard.writeText(JSON.stringify(scanData, null, 2))
        .then(() => showToast("Results copied to clipboard.", "success"))
        .catch(() => showToast("Unable to copy.", "error"));
}

// ── EDITOR ──────────────────────────────────────────────────────────────
function initEditor() {
    const editor = $("script-editor");
    const saved = localStorage.getItem("akila_script");
    editor.value = saved || DEFAULT_SCRIPT;
    updateLineNumbers();
    updateEditorStats();

    editor.addEventListener("input", () => {
        updateLineNumbers();
        updateEditorStats();
    });

    editor.addEventListener("scroll", () => {
        $("line-numbers").scrollTop = editor.scrollTop;
    });

    editor.addEventListener("keydown", (e) => {
        if (e.key === "Tab") {
            e.preventDefault();
            const start = editor.selectionStart;
            const end = editor.selectionEnd;
            editor.value = editor.value.substring(0, start) + "    " + editor.value.substring(end);
            editor.selectionStart = editor.selectionEnd = start + 4;
            updateLineNumbers();
        }
    });
}

function updateLineNumbers() {
    const editor = $("script-editor");
    const lines = editor.value.split("\n").length;
    const nums = $("line-numbers");
    nums.textContent = Array.from({ length: lines }, (_, i) => i + 1).join("\n");
}

function updateEditorStats() {
    const editor = $("script-editor");
    const lines = editor.value.split("\n").length;
    const chars = editor.value.length;
    $("editor-stats").textContent = `Lines: ${lines} · Characters: ${chars.toLocaleString("en-US")}`;
}

function saveScript() {
    const editor = $("script-editor");
    localStorage.setItem("akila_script", editor.value);
    const saved = $("editor-saved");
    saved.style.display = "flex";
    showToast("Script saved locally.", "success");
    setTimeout(() => saved.style.display = "none", 3000);
}

function resetScript() {
    if (confirm("Reset script to original code?")) {
        $("script-editor").value = DEFAULT_SCRIPT;
        localStorage.removeItem("akila_script");
        updateLineNumbers();
        updateEditorStats();
        showToast("Script reset to original.", "info");
    }
}

function downloadScript() {
    const content = $("script-editor").value;
    const blob = new Blob([content], { type: "text/x-python" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "AKILA_prospect.py";
    a.click();
    URL.revokeObjectURL(url);
    showToast("Script downloaded!", "success");
}

// ── EVENT LISTENERS ─────────────────────────────────────────────────────
document.addEventListener("DOMContentLoaded", () => {
    // Tab switching
    document.querySelectorAll(".nav-tab").forEach(tab => {
        tab.addEventListener("click", () => {
            document.querySelectorAll(".nav-tab").forEach(t => t.classList.remove("active"));
            tab.classList.add("active");
            const target = tab.dataset.tab;
            document.querySelectorAll(".tab-content").forEach(tc => tc.style.display = "none");
            $(`tab-${target}`).style.display = "";
        });
    });

    // Type toggle
    $("btn-tertiaire").addEventListener("click", () => { currentType = "T"; updateToggle(); });
    $("btn-particulier").addEventListener("click", () => { currentType = "P"; updateToggle(); });

    // Search
    $("btn-search").addEventListener("click", runScan);
    $("input-entree").addEventListener("keydown", (e) => { if (e.key === "Enter") runScan(); });

    // Example chips
    document.querySelectorAll(".example-chip").forEach(chip => {
        chip.addEventListener("click", () => {
            $("input-entree").value = chip.dataset.example;
        });
    });

    // Raw data toggle
    $("toggle-raw").addEventListener("click", () => {
        const raw = $("data-raw");
        const title = $("toggle-raw");
        const visible = raw.style.display !== "none";
        raw.style.display = visible ? "none" : "";
        title.classList.toggle("open", !visible);
    });

    // Export
    $("btn-export-json").addEventListener("click", exportJSON);
    $("btn-copy-results").addEventListener("click", copyResults);

    // Editor
    $("btn-save-script").addEventListener("click", saveScript);
    $("btn-reset-script").addEventListener("click", resetScript);
    $("btn-download-script").addEventListener("click", downloadScript);

    // Initialize
    initEditor();
    renderHistory();
});
