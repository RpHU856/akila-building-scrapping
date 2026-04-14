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
    
    const offset = 0.004;
    const initialBbox = `${res.lon - offset},${res.lat - offset},${res.lon + offset},${res.lat + offset}`;
    
    selectedRnbIds.clear();
    if (res.rnb_id) selectedRnbIds.add(res.rnb_id);
    centerRnbId = res.rnb_id;
    updateMapActions();
    
    let drawnRnbIds = new Set();
    geoJsonLayer = L.geoJSON(null, {
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

    async function loadBuildingsInBbox(bboxStr) {
        try {
            let apiUrl = `${API.RNB_BASE}/?bbox=${bboxStr}&with_plots=1&limit=50`;
            while (apiUrl) {
                const buildingsData = await fetchJSON(apiUrl);
                if (!buildingsData || !buildingsData.results) break;
                
                const newFeatures = [];
                for (const r of buildingsData.results) {
                    if (r.shape && r.rnb_id && !drawnRnbIds.has(r.rnb_id)) {
                        drawnRnbIds.add(r.rnb_id);
                        newFeatures.push({
                            type: "Feature",
                            geometry: r.shape,
                            properties: { rnb_id: r.rnb_id, status: r.status }
                        });
                    }
                }
                if (newFeatures.length > 0) {
                    geoJsonLayer.addData(newFeatures);
                }
                apiUrl = buildingsData.next || null;
            }
        } catch(e) {
            console.error("Map load error", e);
        }
    }

    // Initial load
    await loadBuildingsInBbox(initialBbox);

    // Dynamic loading on pan/zoom
    leafletMap.on('moveend', () => {
        if (leafletMap.getZoom() >= 16) {
            const bounds = leafletMap.getBounds();
            const bboxStr = `${bounds.getWest()},${bounds.getSouth()},${bounds.getEast()},${bounds.getNorth()}`;
            loadBuildingsInBbox(bboxStr);
        }
    });
}

function updateMapActions() {
    const count = selectedRnbIds.size;
    $("map-selection-info").textContent = `${count} building(s) selected`;
    $("btn-run-multi-scan").style.display = count > 0 ? "block" : "none";
    $("btn-run-multi-scan").textContent = count > 1 ? `Multi-Scan (${count})` : "Scan";
}

// Attach listener
$("btn-run-multi-scan").addEventListener("click", runMultiScan);

async function fetchBuildingData(rnbId, type, adresseLabel, cleBan) {
    const res = await resoudreEntree(rnbId);
    const batId = res.bat_id_bdnb;
    if (!batId) return { res, error: "BDNB missing" };
    
    const p1 = { batiment_groupe_id: `eq.${batId}`, limit: 1 };
    const pm = { batiment_groupe_id: `eq.${batId}`, order: "millesime.desc", limit: 4 };

    const [dBase, dUsage, dFfo, dTopo, dProp, dRisque, dReseau, dDpe, dElec, dGaz] = await Promise.all([
        bdnbQuery("batiment_groupe", { ...p1, select: "code_commune_insee,libelle_commune_insee,code_iris,s_geom_groupe" }),
        bdnbQuery("batiment_groupe_synthese_propriete_usage", { ...p1, select: "usage_principal_bdnb_open" }),
        bdnbQuery("batiment_groupe_ffo_bat", { ...p1, select: "annee_construction,mat_mur_txt,mat_toit_txt,usage_niveau_1_txt,nb_niveau,nb_log" }),
        bdnbQuery("batiment_groupe_bdtopo_bat", { ...p1, select: "hauteur_mean,altitude_sol_mean,l_usage_1" }),
        bdnbQuery("batiment_groupe_proprietaire", { ...p1, select: "bat_prop_denomination_proprietaire" }),
        bdnbQuery("batiment_groupe_risques", { ...p1, select: "alea_argile,alea_radon,alea_sismique" }),
        bdnbQuery("batiment_groupe_indicateur_reseau_chaud_froid", { ...p1, select: "indicateur_distance_au_reseau,reseau_en_construction" }),
        type === "T"
            ? bdnbQuery("batiment_groupe_dpe_tertiaire", { ...p1, select: "identifiant_dpe,classe_conso_energie_dpe_tertiaire,classe_emission_ges_dpe_tertiaire,conso_dpe_tertiaire_ep_m2,emission_ges_dpe_tertiaire_m2,type_energie_chauffage,date_etablissement_dpe,surface_utile" })
            : bdnbQuery("batiment_groupe_dpe_representatif_logement", { ...p1, select: "identifiant_dpe,classe_bilan_dpe,classe_emission_ges,conso_5_usages_ep_m2,emission_ges_5_usages_m2,type_energie_chauffage,date_etablissement_dpe,surface_habitable_immeuble" }),
        bdnbQuery("batiment_groupe_dle_elec_multimillesime", { ...pm, select: "millesime,nb_pdl_tot,conso_tot" }),
        bdnbQuery("batiment_groupe_dle_gaz_multimillesime", { ...pm, select: "millesime,nb_pdl_tot,conso_tot" }),
    ]);

    const base = first(dBase), usage = first(dUsage), ffo = first(dFfo),
          topo = first(dTopo), prop = first(dProp), risque = first(dRisque),
          reseau = first(dReseau), dpe = first(dDpe);

    const elecs = dElec?.length ? dElec : [{millesime: "2022"}], gazs = dGaz?.length ? dGaz : [{millesime: "2022"}];
    const elec = elecs[0], gaz = gazs[0];

    const ademe = await collecterADEME(rnbId, res.adresse_label || adresseLabel, res.cle_ban || cleBan, type, val(dpe?.identifiant_dpe));

    return {
        res, base, usage, ffo, topo, prop, risque, reseau, dpe, elec, gaz, elecs, gazs, ademe: ademe.resultats
    };
}

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

async function collecterSIRENE(lat, lon, rayon = 0.08) {
    if (!lat || !lon) return [];
    const d = await apiFetch("https://recherche-entreprises.api.gouv.fr/search", { lat, long: lon, radius: rayon, per_page: 5 });
    return (d?.results || []).slice(0, 5).map(e => ({
        siret:    e.siret || "—",
        nom:      e.nom_complet || e.nom_raison_sociale || "—",
        naf_code: e.activite_principale || "—",
        naf_lib:  e.libelle_activite_principale || "—",
        effectif: e.tranche_effectif_salarie || "—",
        statut:   e.etat_administratif || "—",
    }));
}

async function collecterEducation(lat, lon, rayon = 300) {
    if (!lat || !lon) return [];
    const d = await apiFetch("https://data.education.gouv.fr/api/explore/v2.1/catalog/datasets/fr-en-annuaire-education/records", {
        where: `within_distance(coordonnees_gps, geom'POINT(${lon} ${lat})', ${rayon}m)`,
        limit: 5,
        select: "identifiant_de_l_etablissement,nom_etablissement,type_etablissement,adresse_1,code_postal,nom_commune,telephone,statut_public_prive,nombre_d_eleves"
    });
    return d?.results || [];
}

async function collecterDVF(lat, lon, rayon = 150) {
    if (!lat || !lon) return [];
    const d = await apiFetch("https://apicarto.ign.fr/api/dvf/mutation", { lon, lat, dist: rayon, limit: 5 });
    return (d?.features || []).slice(0, 5).map(f => f.properties || {});
}

async function collecterBenchmark(codeCommune, hint) {
    const r = {};
    if (codeCommune) {
        const d = await apiFetch("https://data.ademe.fr/data-fair/api/v1/datasets/dpe-conso-tertiaire-par-commune/lines", { q: codeCommune, size: 1 });
        if (d?.results?.[0]) r.commune = d.results[0];
    }
    if (hint) {
        const d = await apiFetch("https://data.ademe.fr/data-fair/api/v1/datasets/dpe-conso-tertiaire-par-activite/lines", { q: hint, size: 3 });
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
    const [ademeRes, dGeo, dFcu, dSirene, dEdu, dDvf, dBench] = await Promise.all([
        collecterADEME(rnbId, res.adresse_label || adresseLabel, res.cle_ban || cleBan, bType, val(dpe?.identifiant_dpe)),
        collecterGeoriques(lat, lon, codeInsee),
        collecterFCU(lat, lon),
        collecterSIRENE(lat, lon),
        collecterEducation(lat, lon),
        collecterDVF(lat, lon),
        collecterBenchmark(codeInsee, null),
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

async function collecterEducation(lat, lon, rayon = 300) {
    if (!lat || !lon) return [];
    const d = await apiFetch("https://data.education.gouv.fr/api/explore/v2.1/catalog/datasets/fr-en-annuaire-education/records", {
        where: `within_distance(coordonnees_gps, geom'POINT(${lon} ${lat})', ${rayon}m)`,
        limit: 5,
        select: "identifiant_de_l_etablissement,nom_etablissement,type_etablissement,adresse_1,code_postal,nom_commune,telephone,statut_public_prive,nombre_d_eleves"
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
        geo(`${base}/argiles`,              { latlon: `${lon},${lat}`, rayon: 100 }),
        geo(`${base}/zonage-sismique`,      { latlon: `${lon},${lat}`, rayon: 100 }),
        geo(`${base}/azi`,                  { latlon: `${lon},${lat}`, rayon: 200 }),
        geo(`${base}/cavites`,              { latlon: `${lon},${lat}`, rayon: 500 }),
        geo(`${base}/installations-classees`,{ latlon: `${lon},${lat}`, rayon: 500 }),
        codeInsee ? geo(`${base}/gaspar/catnat`, { code_insee_commune: codeInsee, page: 1, page_size: 5 }) : Promise.resolve(null),
        codeInsee ? geo(`${base}/radon`,         { code_insee: codeInsee }) : Promise.resolve(null),
    ]);
    const r = {};
    // Argile
    if (dArgile?.data?.[0]) { r.argile_alea = dArgile.data[0].lib_risque_jo || dArgile.data[0].code_alea || "—"; r.argile_code = dArgile.data[0].code_alea || "—"; }
    // Sismique
    if (dSismo?.data?.[0])  { r.sismique_zone = dSismo.data[0].zone || dSismo.data[0].code_zone || "—"; r.sismique_lib = dSismo.data[0].lib_zone || "—"; }
    // Radon
    if (dRadon?.data?.[0])  r.radon_classe = dRadon.data[0].classe_potentiel || "—";
    // Inondation — matches Python key: inondation_nb_zones
    r.inondation_nb_zones = dAzi?.data?.length || 0;
    r.inondation_detail   = dAzi?.data?.slice(0,3).map(x => x.lib_type_alea || x.typeAlea || "").join(", ") || "Aucune zone";
    // Cavités
    r.cavites_nb    = dCavites?.data?.length || 0;
    r.cavites_types = [...new Set((dCavites?.data || []).slice(0,5).map(x => x.typeCavite || ""))].join(", ") || "—";
    // ICPE — matches Python key: icpe_rayon_500m
    r.icpe_rayon_500m = dIcpe?.data?.length || 0;
    r.icpe_noms       = (dIcpe?.data || []).slice(0,3).map(x => x.raisonSociale || x.nomEtab || "").join(", ") || "—";
    // CatNat
    r.catnat_nb      = dCatnat?.total || dCatnat?.data?.length || 0;
    r.catnat_types   = [...new Set((dCatnat?.data || []).slice(0,5).map(x => x.libDomCatNat || ""))].join(", ") || "—";
    r.catnat_derniere = dCatnat?.data?.[0]?.datFin || dCatnat?.data?.[0]?.dateDeb || "—";
    return r;
}

async function collecterFCU(lat, lon) {
    if (!lat || !lon) return null;
    const d = await apiFetch("https://france-chaleur-urbaine.beta.gouv.fr/api/v1/eligibility", { lat, lon });
    if (!d) return null;
    // Keys match Python: fcu_ prefix
    return {
        fcu_eligible:   d.isEligible ?? d.eligible ?? false,
        fcu_distance_m: d.distance ?? d.distanceToNetwork ?? "—",
        fcu_reseau_nom: d.networkName ?? d.nom ?? "—",
        fcu_reseau_id:  d.networkId ?? d.identifiant_reseau ?? "—",
        fcu_enr_pct:    d.tauxENRR ?? "—",
        fcu_co2:        d.emissionCO2 ?? "—",
    };
}

async function collecterSIRENE(lat, lon, rayon = 0.08) {
    if (!lat || !lon) return [];
    const d = await apiFetch("https://recherche-entreprises.api.gouv.fr/search", { lat, long: lon, radius: rayon, per_page: 10 });
    return (d?.results || []).slice(0, 10).map(e => ({
        siret:       e.siret || "—",
        nom:         e.nom_complet || e.nom_raison_sociale || "—",
        naf_code:    e.activite_principale || "—",
        naf_libelle: e.libelle_activite_principale || "—",
        adresse:     e.adresse || "—",
        effectif:    e.tranche_effectif_salarie || "—",
        statut:      e.etat_administratif || "—",
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
