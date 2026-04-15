"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  AKILA_prospect_v2.py — Scanner Full-Stack Bâtiment  (version 2.1)         ║
║                                                                              ║
║  CORRECTIONS v2.1                                                            ║
║    • ADEME conso tertiaire : slugs corrigés (consommation-tertiaire-*)      ║
║    • DVF : supprimé apicarto inexistant → tables BDNB natives               ║
║    • SITADEL : noms de champs corrigés (date_reelle_autorisation…)          ║
║    • Propriétaire : table rel_batiment_groupe_proprietaire_siren_open        ║
║                                                                              ║
║  NOUVELLES TABLES BDNB (v2.1)                                               ║
║    • batiment_groupe_dvf_open_statistique   DVF stats (prix/m², mutations)  ║
║    • batiment_groupe_dvf_open_representatif Dernière transaction DVF        ║
║    • rel_batiment_groupe_dpe_tertiaire_complet Consos vecteur détaillées    ║
║    • rel_batiment_groupe_dpe_logement_complet  Déperditions détaillées      ║
║    • batiment_groupe_urbanisme              MH, PLU patrimonial              ║
║    • batiment_groupe_bpe                   Équipements INSEE (BPE)          ║
║    • batiment_groupe_rnc                   Registre Copropriétés            ║
║    • batiment_groupe_rpls                  Logements sociaux                ║
║    • batiment_groupe_qpv                   Quartier Prioritaire Ville       ║
║    • batiment_groupe_hthd                  Très Haut Débit ARCEP            ║
║    • batiment_groupe_geospx                Fiabilité géocodage              ║
║    • batiment_groupe_bdtopo_zoac           Zones d'activité                 ║
║    • batiment_groupe_bdtopo_equ            Équipements BD TOPO              ║
║                                                                              ║
║  NOUVELLES APIs EXTERNES (v2.1)                                             ║
║    • API Carto GPU (IGN)  Zone PLU, prescriptions, servitudes               ║
║    • API Entreprise DINUM (habilitation)  Liasses fiscales, Qualibat        ║
║                                                                              ║
║  Prérequis : pip install requests pandas openpyxl                           ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import requests, json, sys, ast, os, re
from datetime import datetime
import unicodedata

# ── URLS VÉRIFIÉES ────────────────────────────────────────────────────────────
RNB_BASE        = "https://rnb-api.beta.gouv.fr/api/alpha/buildings"
BAN_URL         = "https://api-adresse.data.gouv.fr/search/"
BDNB_URL        = "https://api.bdnb.io/v1/bdnb/donnees"

# ADEME — DPE open data
ADEME_TERT_OLD  = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe-tertiaire/lines"
ADEME_TERT_NEW  = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe01tertiaire/lines"
ADEME_LOG_OLD   = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe-france/lines"
ADEME_LOG_EXIST = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe03existant/lines"
ADEME_LOG_NEUF  = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe02neuf/lines"
OBS_BASE        = "https://observatoire-dpe-audit.ademe.fr"

# ADEME — Conso tertiaire (slugs CORRIGÉS v2.1)
ADEME_CONSO_ACT  = "https://data.ademe.fr/data-fair/api/v1/datasets/consommation-tertiaire-activite/lines"
ADEME_CONSO_COM  = "https://data.ademe.fr/data-fair/api/v1/datasets/consommation-tertiaire-commune/lines"
ADEME_CONSO_VECT = "https://data.ademe.fr/data-fair/api/v1/datasets/consommation-tertiaire-vecteur-energetique/lines"

# APIs externes
GEORISQUES_BASE = "https://georisques.gouv.fr/api/v1"
FCU_BASE        = "https://france-chaleur-urbaine.beta.gouv.fr/api"  # base sans /v1
GPU_BASE        = "https://apicarto.ign.fr/api/gpu"                  # API Carto Géoportail Urbanisme
SIRENE_BASE     = "https://recherche-entreprises.api.gouv.fr/search"
EDU_URL         = "https://data.education.gouv.fr/api/explore/v2.1/catalog/datasets/fr-en-annuaire-education/records"
API_ENT_BASE    = "https://entreprise.api.gouv.fr/v3"               # API Entreprise DINUM (habilitation)

HEADERS = {"User-Agent": "AKILA-Prospect/2.1", "Accept": "application/json"}
SEP  = "─" * 72
SEP2 = "═" * 72


# ─────────────────────────────────────────────────────────────────────────────
# UTILITAIRES
# ─────────────────────────────────────────────────────────────────────────────
def get(url, params=None, timeout=20):
    try:
        r = requests.get(url, params=params or {}, headers=HEADERS, timeout=timeout)
        return r.json() if r.ok else None
    except Exception:
        return None

def bdnb(endpoint, params):
    return get(f"{BDNB_URL}/{endpoint}", params) or []

def v(val, unit=""):
    if val in (None, "", "None", r"\N", "\\N", "nan", "NaN", [], {}):
        return "—"
    if isinstance(val, str) and val.startswith("[") and val.endswith("]"):
        try:
            val = ", ".join(str(x) for x in ast.literal_eval(val))
        except Exception:
            pass
    elif isinstance(val, list):
        val = ", ".join(str(x) for x in val)
    s = str(val).strip()
    return f"{s} {unit}".strip() if unit and s != "—" else s

def slugifier(texte):
    nfd = unicodedata.normalize("NFD", texte.lower())
    ascii_only = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    return "".join(c if c.isalnum() else "_" for c in ascii_only)[:40]

def titre(texte):
    print(f"\n{SEP2}\n  {texte}\n{SEP2}")

def section(texte):
    print(f"\n  {texte}\n  {'─'*50}")

def eur(val):
    try: return f"{int(float(str(val).replace(',','.'))):,}".replace(",", " ") + " €"
    except Exception: return str(val)

def kWh(val):
    try: return f"{round(float(val)):,}".replace(",", " ") + " kWh"
    except Exception: return str(val)


# ─────────────────────────────────────────────────────────────────────────────
# RÉSOLUTION ENTRÉE
# ─────────────────────────────────────────────────────────────────────────────
def detecter_type_entree(entree):
    e = entree.strip().upper()
    if len(e) == 14 and e.count("-") == 2 and all(len(b) == 4 and b.isalnum() for b in e.split("-")):
        return "rnb"
    if len(e) == 12 and e.isalnum() and not e.isdigit():
        return "rnb"
    if e.count("_") == 2 and all(p.isdigit() for p in e.split("_")):
        return "ban"
    return "adresse"

def resoudre_entree(entree):
    type_entree = detecter_type_entree(entree)
    res = {
        "entree_originale": entree, "type_entree": type_entree,
        "rnb_id": None, "cle_ban": None, "adresse_label": None,
        "bdnb_bat_construction_id": None, "bat_id_bdnb": None,
        "lat": None, "lon": None, "code_commune_insee": None
    }

    def extraire_rnb(data):
        pt = data.get("point", {}).get("coordinates", [])
        if pt: res["lon"], res["lat"] = pt[0], pt[1]
        addrs = data.get("addresses", [])
        if addrs:
            a = addrs[0]
            res["adresse_label"] = f"{a.get('street_number','')} {a.get('street','')} {a.get('city_zipcode','')} {a.get('city_name','')}".strip()
            res["cle_ban"] = a.get("id")
            if res["cle_ban"] and "_" in res["cle_ban"]:
                res["code_commune_insee"] = res["cle_ban"].split("_")[0]
        for ext in data.get("ext_ids", []):
            if ext.get("source") == "bdnb":
                res["bdnb_bat_construction_id"] = ext.get("id")

    if type_entree == "rnb":
        rnb_propre = f"{entree[:4]}-{entree[4:8]}-{entree[8:]}" if len(entree) == 12 else entree
        data = get(f"{RNB_BASE}/{rnb_propre}/", {"with_plots": 1}) or get(f"{RNB_BASE}/{entree}/", {"with_plots": 1})
        if data:
            res["rnb_id"] = data.get("rnb_id") or rnb_propre
            extraire_rnb(data)
    elif type_entree == "ban":
        res["cle_ban"] = entree
        data = get(f"{RNB_BASE}/address/", {"cle_interop_ban": entree, "limit": 1})
        if data and data.get("results"):
            res["rnb_id"] = data["results"][0].get("rnb_id")
            extraire_rnb(data["results"][0])
        if not res["adresse_label"]:
            ban = get(BAN_URL, {"q": entree.replace("_", " "), "limit": 1})
            if ban and ban.get("features"):
                f = ban["features"][0]
                res["lat"], res["lon"] = f["geometry"]["coordinates"][1], f["geometry"]["coordinates"][0]
                res["adresse_label"] = f["properties"].get("label")
                res["code_commune_insee"] = f["properties"].get("citycode")
    else:
        ban = get(BAN_URL, {"q": entree, "limit": 1, "type": "housenumber"}) or get(BAN_URL, {"q": entree, "limit": 1})
        if ban and ban.get("features"):
            p = ban["features"][0]["properties"]
            coords = ban["features"][0]["geometry"]["coordinates"]
            res["lat"], res["lon"] = coords[1], coords[0]
            res["cle_ban"], res["adresse_label"] = p.get("id"), p.get("label")
            res["code_commune_insee"] = p.get("citycode")
            rnb_data = get(f"{RNB_BASE}/address/", {"cle_interop_ban": res["cle_ban"], "limit": 1})
            if rnb_data and rnb_data.get("results"):
                res["rnb_id"] = rnb_data["results"][0].get("rnb_id")
                extraire_rnb(rnb_data["results"][0])

    if res["bdnb_bat_construction_id"]:
        link = bdnb("batiment_construction", {"batiment_construction_id": f"eq.{res['bdnb_bat_construction_id']}", "select": "batiment_groupe_id", "limit": 1})
        if link: res["bat_id_bdnb"] = link[0].get("batiment_groupe_id")

    if not res["bat_id_bdnb"] and res["cle_ban"]:
        for table, champ in [
            ("batiment_groupe_adresse", "cle_interop_adr_principale_ban"),
            ("rel_batiment_groupe_adresse", "cle_interop_adr")
        ]:
            rows = bdnb(table, {champ: f"eq.{res['cle_ban']}", "select": "batiment_groupe_id", "limit": 1})
            if rows: res["bat_id_bdnb"] = rows[0].get("batiment_groupe_id"); break

    return res


# ─────────────────────────────────────────────────────────────────────────────
# GÉORISQUES (API v1 BRGM — sans token)
# ─────────────────────────────────────────────────────────────────────────────
def collecter_georisques(lat, lon, code_insee=None):
    if not lat or not lon: return {}
    result = {}
    latlon = f"{lon},{lat}"

    d = get(f"{GEORISQUES_BASE}/argiles", {"latlon": latlon, "rayon": 100})
    if d and d.get("data"):
        r = d["data"][0]
        result["argile_alea"] = r.get("lib_risque_jo", r.get("code_alea", "—"))
        result["argile_code"] = r.get("code_alea", "—")

    d = get(f"{GEORISQUES_BASE}/zonage-sismique", {"latlon": latlon, "rayon": 100})
    if d and d.get("data"):
        r = d["data"][0]
        result["sismique_zone"] = r.get("zone", r.get("code_zone", "—"))
        result["sismique_lib"]  = r.get("lib_zone", "—")

    if code_insee:
        d = get(f"{GEORISQUES_BASE}/radon", {"code_insee": code_insee})
        if d and d.get("data"):
            result["radon_classe"] = d["data"][0].get("classe_potentiel", "—")

    d = get(f"{GEORISQUES_BASE}/azi", {"latlon": latlon, "rayon": 200})
    if d and d.get("data"):
        result["inondation_nb_zones"] = len(d["data"])
        result["inondation_detail"]   = ", ".join(r.get("lib_type_alea", r.get("typeAlea", "")) for r in d["data"][:3]) or "—"
    else:
        result["inondation_nb_zones"] = 0
        result["inondation_detail"]   = "Aucune zone"

    d = get(f"{GEORISQUES_BASE}/cavites", {"latlon": latlon, "rayon": 500})
    if d and d.get("data"):
        result["cavites_nb"]    = len(d["data"])
        result["cavites_types"] = ", ".join(set(r.get("typeCavite", "") for r in d["data"][:5])) or "—"
    else:
        result["cavites_nb"] = 0

    if code_insee:
        d = get(f"{GEORISQUES_BASE}/gaspar/catnat", {"code_insee_commune": code_insee, "page": 1, "page_size": 5})
        if d and d.get("data"):
            result["catnat_nb"]       = d.get("total", len(d["data"]))
            result["catnat_types"]    = ", ".join(set(r.get("libDomCatNat", "") for r in d["data"][:5])) or "—"
            result["catnat_derniere"] = d["data"][0].get("datFin", d["data"][0].get("dateDeb", "—"))
        else:
            result["catnat_nb"] = 0

    d = get(f"{GEORISQUES_BASE}/installations-classees", {"latlon": latlon, "rayon": 500})
    if d and d.get("data"):
        result["icpe_rayon_500m"] = len(d["data"])
        result["icpe_noms"]       = ", ".join(r.get("raisonSociale", r.get("nomEtab", "")) for r in d["data"][:3]) or "—"
    else:
        result["icpe_rayon_500m"] = 0

    return result


# ─────────────────────────────────────────────────────────────────────────────
# NOUVEAU v2.1 — API Carto GPU : zone PLU, prescriptions, servitudes
# ─────────────────────────────────────────────────────────────────────────────
def collecter_gpu(lat, lon):
    """
    API Carto GPU (IGN) — Géoportail de l'Urbanisme.
    Paramètre geom = GeoJSON Point {"type":"Point","coordinates":[lon,lat]}
    Couches : zone-urba, prescription-surf, assiette-sup-s
    Sans clé API (accès libre).
    """
    if not lat or not lon: return {}
    geom = json.dumps({"type": "Point", "coordinates": [lon, lat]})
    result = {}

    # Zone PLU
    d = get(f"{GPU_BASE}/zone-urba", {"geom": geom})
    if d and d.get("features"):
        feats = d["features"]
        result["plu_nb_zones"]    = len(feats)
        result["plu_types"]       = ", ".join(set(f["properties"].get("typezone", "") for f in feats)) or "—"
        result["plu_libelles"]    = ", ".join(set(f["properties"].get("libelle", "") for f in feats[:3])) or "—"
        # Première zone : infos détaillées
        p = feats[0]["properties"]
        result["plu_typezone"]    = p.get("typezone", "—")
        result["plu_libelle"]     = p.get("libelle", "—")
        result["plu_destdomi"]    = p.get("destdomi", "—")   # destination dominante
        result["plu_partition"]   = p.get("partition", "—")  # identifiant document d'urbanisme
    else:
        result["plu_nb_zones"] = 0

    # Prescriptions surfaciques (contraintes particulières)
    d = get(f"{GPU_BASE}/prescription-surf", {"geom": geom})
    if d and d.get("features"):
        feats = d["features"]
        result["plu_prescriptions_nb"]     = len(feats)
        result["plu_prescriptions"]        = ", ".join(set(
            f["properties"].get("libelle", f["properties"].get("typepsc", "")) for f in feats[:5]
        )) or "—"
    else:
        result["plu_prescriptions_nb"] = 0

    # Assiettes de Servitudes d'Utilité Publique (SUP)
    d = get(f"{GPU_BASE}/assiette-sup-s", {"geom": geom})
    if d and d.get("features"):
        feats = d["features"]
        result["sup_nb"]      = len(feats)
        result["sup_libelles"] = ", ".join(set(
            f["properties"].get("libelle", f["properties"].get("nomservit", "")) for f in feats[:5]
        )) or "—"
    else:
        result["sup_nb"] = 0

    return result


# ─────────────────────────────────────────────────────────────────────────────
# FRANCE CHALEUR URBAINE
# ─────────────────────────────────────────────────────────────────────────────
def collecter_fcu(lat, lon):
    if not lat or not lon: return {}
    d = get(f"{FCU_BASE}/v1/eligibility", {"lat": lat, "lon": lon})
    if not d: return {}
    return {
        "fcu_eligible":   d.get("isEligible", d.get("eligible", False)),
        "fcu_distance_m": d.get("distance", d.get("distanceToNetwork", "—")),
        "fcu_reseau_nom": d.get("networkName", d.get("nom", "—")),
        "fcu_reseau_id":  d.get("networkId", d.get("identifiant_reseau", "—")),
        "fcu_enr_pct":    d.get("tauxENRR", "—"),
        "fcu_co2":        d.get("emissionCO2", "—"),
    }


# ─────────────────────────────────────────────────────────────────────────────
# SIRENE
# ─────────────────────────────────────────────────────────────────────────────
def collecter_sirene(lat, lon, rayon_m=80):
    if not lat or not lon: return []
    d = get(SIRENE_BASE, {"lat": lat, "long": lon, "radius": rayon_m / 1000, "per_page": 5})
    if not d or not d.get("results"): return []
    return [{
        "siret":       e.get("siret", "—"),
        "nom":         e.get("nom_complet", e.get("nom_raison_sociale", "—")),
        "naf_code":    e.get("activite_principale", "—"),
        "naf_libelle": e.get("libelle_activite_principale", "—"),
        "effectif":    e.get("tranche_effectif_salarie", "—"),
        "statut":      e.get("etat_administratif", "—"),
    } for e in d["results"][:5]]


# ─────────────────────────────────────────────────────────────────────────────
# ANNUAIRE ÉDUCATION NATIONALE
# ─────────────────────────────────────────────────────────────────────────────
def collecter_education(lat, lon, rayon_m=300):
    if not lat or not lon: return []
    d = get(EDU_URL, {
        "where": f"within_distance(coordonnees_gps, geom'POINT({lon} {lat})', {rayon_m}m)",
        "limit": 5,
        "select": "identifiant_de_l_etablissement,nom_etablissement,type_etablissement,"
                  "adresse_1,code_postal,nom_commune,telephone,statut_public_prive,nombre_d_eleves"
    })
    if not d or not d.get("results"): return []
    return d["results"]


# ─────────────────────────────────────────────────────────────────────────────
# BENCHMARK TERTIAIRE ADEME (URLs CORRIGÉES v2.1)
# ─────────────────────────────────────────────────────────────────────────────
def collecter_benchmark_tertiaire(code_commune, activite_hint=None):
    result = {"commune": None, "activite": None}
    if code_commune:
        d = get(ADEME_CONSO_COM, {"q": code_commune, "size": 1})
        if d and d.get("results"):
            result["commune"] = d["results"][0]
    if activite_hint:
        d = get(ADEME_CONSO_ACT, {"q": activite_hint, "size": 3})
        if d and d.get("results"):
            result["activite"] = d["results"]
    return result


# ─────────────────────────────────────────────────────────────────────────────
# ADEME DPE OPEN DATA
# ─────────────────────────────────────────────────────────────────────────────
def collecter_ademe_direct(rnb_id, adresse_label, cle_ban, type_cible, numero_dpe_connu=None):
    resultats, erreurs = [], set()
    urls_tert  = [ADEME_TERT_NEW, ADEME_TERT_OLD]
    urls_log   = [ADEME_LOG_EXIST, ADEME_LOG_NEUF, ADEME_LOG_OLD]
    urls_toutes = urls_tert + urls_log if type_cible == "T" else urls_log + urls_tert
    urls_ademe  = urls_tert if type_cible == "T" else urls_log

    def requeter(url, params):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=30)
            if r.ok:
                data = r.json()
                if data and data.get("total", 0) > 0:
                    resultats.extend(data["results"])
                    return True
            else:
                erreurs.add(r.status_code)
        except Exception:
            erreurs.add("TIMEOUT")
        return False

    if numero_dpe_connu and numero_dpe_connu not in ("—", "None", None):
        for url in urls_toutes:
            if requeter(url, {"qs": f'numero_dpe:"{numero_dpe_connu}"', "size": 1}): break
            if not resultats and requeter(url, {"qs": f'Numero_DPE:"{numero_dpe_connu}"', "size": 1}): break
            if not resultats and requeter(url, {"q": numero_dpe_connu, "size": 1}): break

    if not resultats and adresse_label:
        cp = cle_ban.split("_")[0] if cle_ban and "_" in cle_ban else ""
        nom_rue = " ".join(w for w in adresse_label.split() if not w.isdigit() and len(w) > 2)[:40]
        for url in urls_ademe:
            params = {"size": 5}
            if cp: params["qs"] = f'code_postal:"{cp}"'
            if nom_rue: params["q"] = nom_rue
            requeter(url, params)

    vus, uniques = set(), []
    for r in resultats:
        k = r.get("numero_dpe", id(r))
        if k not in vus:
            vus.add(k); uniques.append(r)
    return uniques, erreurs


# ─────────────────────────────────────────────────────────────────────────────
# OBSERVATOIRE DPE — XLS complet
# ─────────────────────────────────────────────────────────────────────────────
def telecharger_xlsx_observatoire(numero_dpe, dossier_sortie=None):
    if not numero_dpe or numero_dpe in ("—", "None", None): return None
    if dossier_sortie is None: dossier_sortie = os.path.dirname(os.path.abspath(__file__))
    chemin = os.path.join(dossier_sortie, f"{numero_dpe}.xlsx")
    if os.path.exists(chemin):
        print(f"  XLS déjà présent : {chemin}")
        return chemin

    session = requests.Session()
    session.headers.update(HEADERS)
    try:
        r = session.get(f"{OBS_BASE}/pub/dpe/{numero_dpe}", timeout=15, allow_redirects=True)
        if r.ok and len(r.content) > 1000:
            for pattern in [r'href=["\']([^"\']*\.xlsx?)["\']', r'"url"\s*:\s*"([^"]*\.xlsx?)"']:
                for match in re.findall(pattern, r.text, re.IGNORECASE):
                    dl_url = match if match.startswith("http") else f"{OBS_BASE}{match}"
                    try:
                        r2 = session.get(dl_url, timeout=20)
                        ct = r2.headers.get("content-type", "")
                        if r2.ok and ("spreadsheet" in ct or "excel" in ct or "octet-stream" in ct):
                            with open(chemin, "wb") as f: f.write(r2.content)
                            print(f"  ✓ XLS téléchargé ({len(r2.content)//1024} Ko)")
                            return chemin
                    except Exception: continue
    except Exception: pass

    for url in [f"{OBS_BASE}/pub/dpe/{numero_dpe}/download", f"{OBS_BASE}/pub/dpe/{numero_dpe}.xlsx"]:
        try:
            r = session.get(url, timeout=15, allow_redirects=True)
            ct = r.headers.get("content-type", "")
            if r.ok and ("spreadsheet" in ct or "excel" in ct or "octet-stream" in ct):
                with open(chemin, "wb") as f: f.write(r.content)
                return chemin
        except Exception: continue

    print(f"  ✗ Observatoire inaccessible (Cloudflare/VPN étranger)")
    print(f"  → Téléchargement manuel : {OBS_BASE}/pub/dpe/{numero_dpe}")
    return None


def analyser_dpe_xlsx(chemin):
    try: import pandas as pd
    except ImportError: return {}
    if not chemin or not os.path.exists(chemin): return {}
    try: xl = pd.read_excel(chemin, sheet_name=None)
    except Exception: return {}
    res = {}
    for feuille in ["administratif", "logement", "logement_sortie"]:
        if feuille in xl:
            df = xl[feuille].dropna(how="all")
            d = {}
            for _, row in df.iterrows():
                k, val = row.iloc[0], (row.iloc[1] if len(row) > 1 else None)
                import pandas as _pd
                if _pd.notna(k) and _pd.notna(val):
                    try: d[str(k).strip()] = float(val) if str(val).replace(".", "").replace("-", "").isdigit() else str(val).strip()
                    except Exception: d[str(k).strip()] = str(val).strip()
            res[feuille] = d
    if "rapport" in xl:
        import pandas as _pd2
        df = xl["rapport"].dropna(how="all")
        rapport = {"descriptif_simplifie": [], "packs_travaux": [], "gestes_entretien": []}
        pack = None
        for _, row in df.iterrows():
            k   = str(row.iloc[0]).strip() if _pd2.notna(row.iloc[0]) else ""
            val = str(row.iloc[1]).strip() if _pd2.notna(row.iloc[1]) else ""
            if "num_pack_travaux" in k:                            pack = {"numero": val, "travaux": []}; rapport["packs_travaux"].append(pack)
            elif "conso_5_usages_apres_travaux" in k and pack:    pack["conso_apres"] = val
            elif "cout_pack_travaux_min" in k and pack:           pack["cout_min"] = val
            elif "cout_pack_travaux_max" in k and pack:           pack["cout_max"] = val
            elif k.startswith("travaux_") and pack and val:       pack["travaux"].append(val)
            elif k.startswith("descriptif_simplifie_") and val:   rapport["descriptif_simplifie"].append(val)
            elif k.startswith("descriptif_geste_entretien_") and val: rapport["gestes_entretien"].append(val)
        res["rapport"] = rapport
    return res


def afficher_dpe_complet(d):
    admin  = d.get("administratif", {})
    logem  = d.get("logement", {})
    sortie = d.get("logement_sortie", {})
    rapport = d.get("rapport", {})

    section("DPE COMPLET — OBSERVATOIRE ADEME (475 variables)")
    print(f"  DPE n°    : {admin.get('reference_interne_projet','—')}  |  Date : {admin.get('date_visite_diagnostiqueur','—')}")
    print(f"  Adresse   : {admin.get('ban_housenumber','')} {admin.get('ban_street','')} — {admin.get('ban_postcode','')} {admin.get('ban_city','')}")
    print(f"  Moteur    : {admin.get('version_moteur_calcul','—')}  |  Zone climatique : {logem.get('zone_climatique','—')}")
    print(f"  Surface   : {logem.get('surface_habitable_logement','—')} m²  |  Inertie : {logem.get('classe_inertie','—')}")
    etiq_e = sortie.get("classe_bilan_dpe","—")
    etiq_g = sortie.get("classe_emission_ges","—")
    conso  = sortie.get("conso_5_usages_m2") or sortie.get("ep_conso_5_usages_m2","—")
    try: cout = f"{round(float(sortie.get('cout_5_usages',0))):,}".replace(",", " ") + " €/an"
    except Exception: cout = "—"
    section("RÉSULTATS 3CL")
    print(f"  Étiquette Énergie : {etiq_e}  ({conso} kWh EP/m²/an)  GES : {etiq_g}")
    print(f"  Coût annuel       : {cout}  |  Type énergie : {sortie.get('type_energie','—')}")
    print(f"  Ubat              : {sortie.get('ubat','—')} W/m².K  |  Confort été : {sortie.get('indicateur_confort_ete','—')}")

    section("DÉPERDITIONS THERMIQUES")
    total = sortie.get("deperdition_enveloppe", 0)
    for label, key in [("Murs","deperdition_mur"),("Plancher haut","deperdition_plancher_haut"),
                        ("Plancher bas","deperdition_plancher_bas"),("Baies vitrées","deperdition_baie_vitree"),
                        ("Ponts thermiques","deperdition_pont_thermique"),("Renouvl. air","deperdition_renouvellement_air"),
                        ("TOTAL","deperdition_enveloppe")]:
        val = sortie.get(key)
        if val is not None:
            try:
                pct = f" ({round(float(val)/float(total)*100)}%)" if total and key != "deperdition_enveloppe" else ""
                print(f"  {label:<22} : {round(float(val),1):>8} W/K{pct}")
            except Exception: print(f"  {label:<22} : {val} W/K")

    if rapport and rapport.get("packs_travaux"):
        section("RECOMMANDATIONS TRAVAUX")
        desc = rapport.get("descriptif_simplifie", [])
        if desc: print(f"  Postes : {', '.join(list(dict.fromkeys(desc))[:6])}")
        for pack in rapport["packs_travaux"]:
            travaux = " + ".join(pack.get("travaux", []))
            print(f"  Pack {pack.get('numero','?')} : {travaux}")
            print(f"    → {pack.get('conso_apres','—')} kWh EP/m²/an  |  {pack.get('cout_min','—')}–{pack.get('cout_max','—')} €/m²")


def telecharger_dossier_complet(numero_dpe, dossier_sortie=None):
    if not numero_dpe or numero_dpe in ("None", "—"): return {}
    if dossier_sortie is None: dossier_sortie = os.path.dirname(os.path.abspath(__file__))
    titre("RÉCUPÉRATION DPE COMPLET")
    chemin = telecharger_xlsx_observatoire(numero_dpe, dossier_sortie)
    if chemin:
        donnees = analyser_dpe_xlsx(chemin)
        if donnees: afficher_dpe_complet(donnees); return donnees

    print(f"  Fallback → API simplifiée data.ademe.fr")
    for nom, url in [("Logements post-2021", ADEME_LOG_EXIST), ("Tertiaire post-2021", ADEME_TERT_NEW),
                      ("Logements neufs", ADEME_LOG_NEUF), ("Tertiaire avant", ADEME_TERT_OLD)]:
        for params in [{"qs": f'numero_dpe:"{numero_dpe}"'}, {"qs": f'Numero_DPE:"{numero_dpe}"'}, {"q": numero_dpe}]:
            try:
                r = requests.get(url, params={**params, "size": 1}, headers=HEADERS, timeout=20)
                if r.ok:
                    data = r.json()
                    if data.get("total", 0) > 0:
                        result = data["results"][0]
                        print(f"  Trouvé dans : {nom}")
                        def c(*cles):
                            for k in cles:
                                val = result.get(k)
                                if val not in (None, "", "None"): return str(val)
                            return "—"
                        print(f"  Étiquette : {c('classe_consommation_energie','Etiquette_DPE','classe_bilan_dpe')}")
                        print(f"  GES       : {c('classe_estimation_ges','Etiquette_GES','classe_emission_ges')}")
                        return result
            except Exception: continue
    print(f"  ✗ → {OBS_BASE}/pub/dpe/{numero_dpe}")
    return {}


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    titre("AKILA PROSPECT v2.1 — SCANNER FULL-STACK BÂTIMENT")
    print(f"  Sources : RNB · BAN · BDNB (30 tables) · Géorisques · GPU PLU · FCU · SIRENE · Éducation · ADEME")

    choix = input("\n  Cible : [T]ertiaire ou [P]articulier ? [T/p] : ").strip().upper()
    type_cible = "P" if choix == "P" else "T"
    hint = None
    if type_cible == "T":
        hint = input("  Type d'activité pour benchmark (ex: lycée / bureau / vide) : ").strip() or None

    entree = input("  Identifiant RNB, clé BAN ou adresse : ").strip()
    if not entree: return

    # ── ÉTAPE 1 : Résolution
    print(f"\n  [1/6] Résolution adresse (BAN + RNB)...")
    res = resoudre_entree(entree)
    bat_id = res.get("bat_id_bdnb")
    if not bat_id:
        print("\n  ✗ Bâtiment introuvable dans la BDNB.")
        return

    lat, lon = res.get("lat"), res.get("lon")
    code_insee = res.get("code_commune_insee")

    # ── ÉTAPE 2 : BDNB — TOUTES les tables (30+)
    print(f"  [2/6] BDNB — extraction complète ({bat_id})...")
    p1 = {"batiment_groupe_id": f"eq.{bat_id}", "limit": 1}
    pm = {"batiment_groupe_id": f"eq.{bat_id}", "order": "millesime.desc", "limit": 4}
    first = lambda lst: lst[0] if lst else {}

    # Tables de base (inchangées)
    d_base    = first(bdnb("batiment_groupe",
                          {**p1, "select": "code_commune_insee,libelle_commune_insee,s_geom_groupe,code_iris,code_epci_insee,quartier_prioritaire,nom_qp"}))
    d_usage   = first(bdnb("batiment_groupe_synthese_propriete_usage",
                          {**p1, "select": "usage_principal_bdnb_open"}))
    d_ffo     = first(bdnb("batiment_groupe_ffo_bat",
                          {**p1, "select": "annee_construction,mat_mur_txt,mat_toit_txt,nb_log,nb_niveau,usage_niveau_1_txt"}))
    d_topo    = first(bdnb("batiment_groupe_bdtopo_bat",
                          {**p1, "select": "hauteur_mean,altitude_sol_mean,l_usage_1,l_usage_2,l_etat,max_hauteur"}))
    # Propriétaire — table corrigée v2.1
    d_prop    = first(bdnb("rel_batiment_groupe_proprietaire_siren_open",
                          {**p1, "select": "bat_prop_denomination_proprietaire,siren,nb_locaux_open,is_bailleur", "limit": 1}))
    d_risque  = first(bdnb("batiment_groupe_risques",
                          {**p1, "select": "alea_argile,alea_radon,alea_sismique"}))
    d_reseau  = first(bdnb("batiment_groupe_indicateur_reseau_chaud_froid",
                          {**p1, "select": "indicateur_distance_au_reseau,reseau_en_construction,id_reseau"}))

    # SITADEL — noms de champs CORRIGÉS v2.1
    d_sitadel = bdnb("sitadel",
                     {**p1, "select": "date_reelle_autorisation,nature_projet,destination_principale,"
                                      "etat_avancement_projet,s_loc_creee,s_loc_demolie", "limit": 5}) or []

    # DPE principal
    if type_cible == "T":
        d_dpe = first(bdnb("batiment_groupe_dpe_tertiaire",
                           {**p1, "select": "identifiant_dpe,classe_conso_energie_dpe_tertiaire,"
                                            "classe_emission_ges_dpe_tertiaire,conso_dpe_tertiaire_ep_m2,"
                                            "emission_ges_dpe_tertiaire_m2,type_energie_chauffage,"
                                            "date_etablissement_dpe,surface_utile,shon,"
                                            "methode_application_dpe_tertiaire"}))
        # DPE tertiaire complet — consos par vecteur (NOUVEAU v2.1)
        d_dpe_complet = first(bdnb("rel_batiment_groupe_dpe_tertiaire_complet",
                                   {**p1, "select": "identifiant_dpe,conso_electricite,conso_gaz,conso_fioul,"
                                                    "conso_bois,conso_reseau_chaleur,conso_reseau_froid,"
                                                    "conso_autre_fossile,conso_gpl_butane_propane,"
                                                    "derniere_annee_consommation,type_energie_climatisation,"
                                                    "type_energie_ecs,categorie_erp_dpe_tertiaire", "limit": 1}))
    else:
        d_dpe = first(bdnb("batiment_groupe_dpe_representatif_logement",
                           {**p1, "select": "identifiant_dpe,classe_bilan_dpe,classe_emission_ges,"
                                            "conso_5_usages_ep_m2,emission_ges_5_usages_m2,"
                                            "type_energie_chauffage,date_etablissement_dpe,"
                                            "surface_habitable_immeuble,classe_inertie"}))
        # DPE logement complet — déperditions par poste (NOUVEAU v2.1)
        d_dpe_complet = first(bdnb("rel_batiment_groupe_dpe_logement_complet",
                                   {**p1, "select": "identifiant_dpe,deperdition_mur,deperdition_plancher_haut,"
                                                    "deperdition_plancher_bas,deperdition_baie_vitree,"
                                                    "deperdition_pont_thermique,deperdition_porte,"
                                                    "type_ventilation,type_isolation_mur_exterieur,"
                                                    "type_isolation_plancher_haut,type_generateur_chauffage,"
                                                    "type_generateur_ecs,surface_mur_exterieur,"
                                                    "pourcentage_surface_baie_vitree_exterieur", "limit": 1}))

    d_elec = bdnb("batiment_groupe_dle_elec_multimillesime", {**pm, "select": "millesime,conso_tot,nb_pdl_tot"})
    d_gaz  = bdnb("batiment_groupe_dle_gaz_multimillesime",  {**pm, "select": "millesime,conso_tot,nb_pdl_tot"})
    d_reseaux = bdnb("batiment_groupe_dle_reseaux_multimillesime",
                     {**pm, "select": "millesime,conso_tot,type_reseau,identifiant_reseau"})

    # ── NOUVELLES TABLES BDNB v2.1 ───────────────────────────────────────────
    # DVF intégré dans la BDNB (remplace apicarto inexistant)
    d_dvf_stat  = first(bdnb("batiment_groupe_dvf_open_statistique",
                             {**p1, "select": "nb_mutation,valeur_fonciere_median,valeur_fonciere_min,"
                                              "valeur_fonciere_max,prix_m2_local_median,prix_m2_local_moyen,"
                                              "nb_locaux_mutee,nb_appartement_mutee,nb_locaux_tertiaire_mutee"}))
    d_dvf_repr  = first(bdnb("batiment_groupe_dvf_open_representatif",
                             {**p1, "select": "date_mutation,valeur_fonciere,prix_m2_local,"
                                              "surface_bati_mutee_tertiaire,surface_bati_mutee_residencielle_collective,"
                                              "nb_locaux_mutee_mutation,nb_piece_principale"}))
    # Urbanisme
    d_urba      = first(bdnb("batiment_groupe_urbanisme",
                             {**p1, "select": "monument_historique,denomination_monument_historique,"
                                              "distance_monument_historique,zone_plu_bati_patrimonial,"
                                              "contrainte_urbanisme_ac1,source_monument_historique"}))
    # BPE — équipements publics proches
    d_bpe       = first(bdnb("batiment_groupe_bpe",
                             {**p1, "select": "l_type_equipement"}))
    # Copropriétés
    d_rnc       = first(bdnb("batiment_groupe_rnc",
                             {**p1, "select": "nb_log,nb_lot_tot,nb_lot_tertiaire,numero_immat_principal,"
                                              "l_nom_copro,periode_construction_max"}))
    # Logements sociaux
    d_rpls      = first(bdnb("batiment_groupe_rpls",
                             {**p1, "select": "nb_log,dans_qpv,classe_ener_principale,classe_ges_principale,"
                                              "accessible_pmr,type_construction"}))
    # QPV
    d_qpv       = first(bdnb("batiment_groupe_qpv",
                             {**p1, "select": "quartier_prioritaire,nom_quartier"}))
    # Très Haut Débit ARCEP
    d_hthd      = first(bdnb("batiment_groupe_hthd",
                             {**p1, "select": "nb_pdl,l_type_pdl,l_nom_pdl"}))
    # Fiabilité géocodage
    d_geospx    = first(bdnb("batiment_groupe_geospx",
                             {**p1, "select": "croisement_geospx_reussi,fiabilite_adresse,fiabilite_emprise_sol,fiabilite_hauteur"}))
    # Zones d'activité BD TOPO
    d_zoac      = first(bdnb("batiment_groupe_bdtopo_zoac",
                             {**p1, "select": "l_nature,l_nature_detaillee,l_toponyme"}))
    # Équipements BD TOPO
    d_equ       = first(bdnb("batiment_groupe_bdtopo_equ",
                             {**p1, "select": "l_nature,l_nature_detaillee,l_toponyme"}))

    numero_dpe = v(d_dpe.get("identifiant_dpe"))

    # ── ÉTAPE 3 : APIs externes
    print(f"  [3/6] Géorisques + GPU PLU + France Chaleur Urbaine...")
    d_geo = collecter_georisques(lat, lon, code_insee)
    d_gpu = collecter_gpu(lat, lon)
    d_fcu = collecter_fcu(lat, lon)

    print(f"  [4/6] SIRENE + Annuaire Éducation...")
    d_sirene = collecter_sirene(lat, lon, rayon_m=80)
    d_edu    = collecter_education(lat, lon, rayon_m=300)

    print(f"  [5/6] Benchmark tertiaire ADEME + DPE open data...")
    d_bench = collecter_benchmark_tertiaire(code_insee or d_base.get("code_commune_insee"), hint)
    donnees_ademe, erreurs_ademe = collecter_ademe_direct(
        res.get("rnb_id"), res.get("adresse_label"), res.get("cle_ban"),
        type_cible, numero_dpe if numero_dpe != "—" else None
    )

    # =========================================================================
    # AFFICHAGE STRUCTURÉ
    # =========================================================================
    adresse_affichee = res.get("adresse_label") or "Adresse non résolue"
    titre(f"CARTE D'IDENTITÉ : {adresse_affichee}")

    section("IDENTIFIANTS OFFICIELS")
    print(f"  ID RNB (Bâtiment)        : {v(res.get('rnb_id'))}")
    print(f"  ID BDNB (CSTB)           : {v(bat_id)}")
    print(f"  Clé BAN (Interop)        : {v(res.get('cle_ban'))}")
    print(f"  Code INSEE commune       : {v(code_insee)}")
    print(f"  Code EPCI                : {v(d_base.get('code_epci_insee'))}")
    if res.get("rnb_id"): print(f"  Fiche RNB                : https://rnb.beta.gouv.fr/batiment/{res['rnb_id']}")
    print(f"  Fiche BDNB               : https://bdnb.io/batiment/{bat_id}")

    section("LOCALISATION")
    print(f"  Commune                  : {v(d_base.get('code_commune_insee'))} — {v(d_base.get('libelle_commune_insee'))}")
    print(f"  Code IRIS                : {v(d_base.get('code_iris'))}")
    print(f"  Coordonnées GPS          : {v(lat)}, {v(lon)}")
    print(f"  Quartier Prioritaire     : {v(d_qpv.get('quartier_prioritaire') or d_base.get('quartier_prioritaire'))}  {v(d_qpv.get('nom_quartier') or d_base.get('nom_qp'))}")

    section("FIABILITÉ GÉOCODAGE (BDNB geospx)")
    print(f"  Croisement géospatial    : {v(d_geospx.get('croisement_geospx_reussi'))}")
    print(f"  Fiabilité adresse        : {v(d_geospx.get('fiabilite_adresse'))}")
    print(f"  Fiabilité emprise sol    : {v(d_geospx.get('fiabilite_emprise_sol'))}")
    print(f"  Fiabilité hauteur        : {v(d_geospx.get('fiabilite_hauteur'))}")

    section("CARACTÉRISTIQUES PHYSIQUES")
    print(f"  Année de construction    : {v(d_ffo.get('annee_construction'))}")
    try: print(f"  Âge du bâtiment          : {2025 - int(d_ffo['annee_construction'])} ans")
    except Exception: pass
    surf = d_base.get("s_geom_groupe")
    print(f"  Surface emprise au sol   : {v(round(float(surf)) if surf else None, 'm²')}")
    print(f"  Hauteur moyenne          : {v(d_topo.get('hauteur_mean'), 'm')}  (max : {v(d_topo.get('max_hauteur'), 'm')})")
    print(f"  Altitude sol             : {v(d_topo.get('altitude_sol_mean'), 'm')}")
    print(f"  Nb niveaux               : {v(d_ffo.get('nb_niveau'))}")
    print(f"  Matériaux murs           : {v(d_ffo.get('mat_mur_txt'))}")
    print(f"  Matériaux toit           : {v(d_ffo.get('mat_toit_txt'))}")
    print(f"  Usages BD TOPO           : {v(d_topo.get('l_usage_1'))}  {v(d_topo.get('l_usage_2'))}")
    print(f"  État BD TOPO             : {v(d_topo.get('l_etat'))}")

    section("USAGE & PROPRIÉTÉ")
    print(f"  Usage principal (BDNB)   : {v(d_usage.get('usage_principal_bdnb_open'))}")
    print(f"  Usage foncier (FF)       : {v(d_ffo.get('usage_niveau_1_txt'))}")
    print(f"  Propriétaire (dénomination): {v(d_prop.get('bat_prop_denomination_proprietaire'))}")
    print(f"  SIREN propriétaire       : {v(d_prop.get('siren'))}")
    print(f"  Est bailleur             : {v(d_prop.get('is_bailleur'))}")
    print(f"  Nb locaux propriétaire   : {v(d_prop.get('nb_locaux_open'))}")

    # NOUVEAU v2.1 — Équipements et zones d'activité
    if d_zoac.get("l_nature"):
        section("ZONES D'ACTIVITÉ BD TOPO")
        print(f"  Nature                   : {v(d_zoac.get('l_nature'))}")
        print(f"  Nature détaillée         : {v(d_zoac.get('l_nature_detaillee'))}")
        print(f"  Toponyme                 : {v(d_zoac.get('l_toponyme'))}")

    if d_equ.get("l_nature"):
        section("ÉQUIPEMENTS BD TOPO À PROXIMITÉ")
        print(f"  Natures                  : {v(d_equ.get('l_nature'))}")
        print(f"  Détails                  : {v(d_equ.get('l_nature_detaillee'))}")

    if d_bpe.get("l_type_equipement"):
        section("ÉQUIPEMENTS INSEE (BPE)")
        print(f"  Types d'équipements      : {v(d_bpe.get('l_type_equipement'))}")

    section("PERFORMANCE ÉNERGÉTIQUE (BDNB)")
    print(f"  Numéro DPE               : {numero_dpe}")
    if type_cible == "T":
        print(f"  Étiquette Énergie        : {v(d_dpe.get('classe_conso_energie_dpe_tertiaire'))}")
        print(f"  Étiquette GES            : {v(d_dpe.get('classe_emission_ges_dpe_tertiaire'))}")
        print(f"  Consommation EP          : {v(d_dpe.get('conso_dpe_tertiaire_ep_m2'), 'kWh/m²/an')}")
        print(f"  Émissions GES            : {v(d_dpe.get('emission_ges_dpe_tertiaire_m2'), 'kg CO2/m²/an')}")
        print(f"  Énergie chauffage        : {v(d_dpe.get('type_energie_chauffage'))}")
        print(f"  Méthode DPE              : {v(d_dpe.get('methode_application_dpe_tertiaire'))}")
        print(f"  Surface utile / SHON     : {v(d_dpe.get('surface_utile'))} m² / {v(d_dpe.get('shon'))} m²")
        print(f"  Catégorie ERP            : {v(d_dpe.get('categorie_erp_dpe_tertiaire') or d_dpe_complet.get('categorie_erp_dpe_tertiaire'))}")
        # Consos détaillées par vecteur (NOUVEAU v2.1)
        if d_dpe_complet:
            section("CONSOS PAR VECTEUR ÉNERGÉTIQUE (DPE tertiaire complet)")
            for label, key in [
                ("Électricité", "conso_electricite"), ("Gaz naturel", "conso_gaz"),
                ("Fioul", "conso_fioul"), ("Bois", "conso_bois"),
                ("Réseau chaleur", "conso_reseau_chaleur"), ("Réseau froid", "conso_reseau_froid"),
                ("GPL/Butane/Propane", "conso_gpl_butane_propane"), ("Autre fossile", "conso_autre_fossile"),
            ]:
                val = d_dpe_complet.get(key)
                if val and str(val) not in ("0", "0.0", "None", "—"):
                    print(f"  {label:<22} : {v(val)} kWh/an")
            print(f"  Énergie climatisation    : {v(d_dpe_complet.get('type_energie_climatisation'))}")
            print(f"  Énergie ECS              : {v(d_dpe_complet.get('type_energie_ecs'))}")
            print(f"  Dernière année conso     : {v(d_dpe_complet.get('derniere_annee_consommation'))}")
    else:
        print(f"  Étiquette Énergie        : {v(d_dpe.get('classe_bilan_dpe'))}")
        print(f"  Étiquette GES            : {v(d_dpe.get('classe_emission_ges'))}")
        print(f"  Conso 5 usages EP        : {v(d_dpe.get('conso_5_usages_ep_m2'), 'kWh/m²/an')}")
        print(f"  Émissions GES            : {v(d_dpe.get('emission_ges_5_usages_m2'), 'kg CO2/m²/an')}")
        print(f"  Surface habitable        : {v(d_dpe.get('surface_habitable_immeuble'), 'm²')}")
        print(f"  Classe inertie           : {v(d_dpe.get('classe_inertie'))}")
        # Déperditions détaillées (NOUVEAU v2.1)
        if d_dpe_complet and any(d_dpe_complet.get(k) for k in ["deperdition_mur","deperdition_baie_vitree"]):
            section("DÉPERDITIONS THERMIQUES (DPE logement complet)")
            for label, key in [
                ("Murs","deperdition_mur"),("Plancher haut","deperdition_plancher_haut"),
                ("Plancher bas","deperdition_plancher_bas"),("Baies vitrées","deperdition_baie_vitree"),
                ("Ponts therm.","deperdition_pont_thermique"),("Portes","deperdition_porte"),
            ]:
                val = d_dpe_complet.get(key)
                if val: print(f"  {label:<22} : {v(val)} W/K")
            print(f"  Isolation murs           : {v(d_dpe_complet.get('type_isolation_mur_exterieur'))}")
            print(f"  Isolation plancher haut  : {v(d_dpe_complet.get('type_isolation_plancher_haut'))}")
            print(f"  Type ventilation         : {v(d_dpe_complet.get('type_ventilation'))}")
            print(f"  Générateur chauffage     : {v(d_dpe_complet.get('type_generateur_chauffage'))}")
            print(f"  Générateur ECS           : {v(d_dpe_complet.get('type_generateur_ecs'))}")
            print(f"  Surface vitrée (%)       : {v(d_dpe_complet.get('pourcentage_surface_baie_vitree_exterieur'))} %")

    print(f"  Date DPE                 : {v(d_dpe.get('date_etablissement_dpe'))}")
    if numero_dpe != "—": print(f"  Observatoire             : {OBS_BASE}/pub/dpe/{numero_dpe}")

    section("CONSOMMATIONS RÉELLES (DLE — SDES / BDNB)")
    print(f"  Électricité :")
    if d_elec:
        vals = []
        for e in d_elec:
            c = e.get("conso_tot")
            vals.append(float(c) if c else 0)
            try: cout = f"[~{round(float(c)*0.15/1000):,} k€/an]".replace(",", " ")
            except Exception: cout = ""
            print(f"    {e.get('millesime','—')}  →  {kWh(c)}  ({v(e.get('nb_pdl_tot'))} PDL)  {cout}")
        if len(vals) >= 2:
            delta = (vals[0]-vals[-1])/vals[-1]*100 if vals[-1] else 0
            print(f"    Tendance : {'↓' if delta<0 else '↑'} {abs(round(delta,1))}%")
    else: print("    Non disponible.")
    print(f"  Gaz :")
    if d_gaz:
        for g in d_gaz:
            try: print(f"    {g.get('millesime','—')}  →  {kWh(g.get('conso_tot'))}  ({v(g.get('nb_pdl_tot'))} PDL)")
            except Exception: print(f"    {g.get('millesime','—')}  →  —")
    else: print("    Non disponible.")
    if d_reseaux:
        print(f"  Réseau chaleur :")
        for r in d_reseaux:
            print(f"    {r.get('millesime','—')}  →  {kWh(r.get('conso_tot'))}  ({v(r.get('type_reseau'))})")

    # DVF BDNB natif (CORRIGÉ v2.1 — remplace apicarto inexistant)
    section("VALEURS FONCIÈRES DVF (BDNB natif)")
    if d_dvf_stat.get("nb_mutation"):
        print(f"  Nb mutations historiques : {v(d_dvf_stat.get('nb_mutation'))}")
        print(f"  Valeur foncière médiane  : {eur(d_dvf_stat.get('valeur_fonciere_median'))}")
        print(f"  Valeur foncière min/max  : {eur(d_dvf_stat.get('valeur_fonciere_min'))} / {eur(d_dvf_stat.get('valeur_fonciere_max'))}")
        print(f"  Prix/m² médian           : {eur(d_dvf_stat.get('prix_m2_local_median'))}")
        print(f"  Prix/m² moyen            : {eur(d_dvf_stat.get('prix_m2_local_moyen'))}")
        print(f"  Locaux tertiaires mutés  : {v(d_dvf_stat.get('nb_locaux_tertiaire_mutee'))}")
    else:
        print("  Aucune transaction DVF enregistrée pour ce bâtiment.")
    if d_dvf_repr.get("date_mutation"):
        print(f"  Dernière transaction     : {v(d_dvf_repr.get('date_mutation'))} — {eur(d_dvf_repr.get('valeur_fonciere'))}")
        print(f"  Prix/m² (dernière)       : {eur(d_dvf_repr.get('prix_m2_local'))}")

    # SITADEL (champs CORRIGÉS v2.1)
    if d_sitadel:
        section(f"PERMIS DE CONSTRUIRE SITADEL ({len(d_sitadel)} enregistrement(s))")
        for pc in d_sitadel:
            print(f"  {v(pc.get('date_reelle_autorisation'))} — {v(pc.get('nature_projet'))} — {v(pc.get('destination_principale'))}")
            if pc.get("etat_avancement_projet"):
                print(f"    État : {pc.get('etat_avancement_projet')}  "
                      f"Surface créée : {v(pc.get('s_loc_creee'))} m²  "
                      f"Surface démolie : {v(pc.get('s_loc_demolie'))} m²")

    # Copropriétés
    if d_rnc.get("nb_lot_tot"):
        section("REGISTRE NATIONAL DES COPROPRIÉTÉS (RNC)")
        print(f"  N° immatriculation       : {v(d_rnc.get('numero_immat_principal'))}")
        print(f"  Nom copropriété          : {v(d_rnc.get('l_nom_copro'))}")
        print(f"  Nb lots total            : {v(d_rnc.get('nb_lot_tot'))}  (tertiaires : {v(d_rnc.get('nb_lot_tertiaire'))})")
        print(f"  Nb logements             : {v(d_rnc.get('nb_log'))}")
        print(f"  Période construction     : {v(d_rnc.get('periode_construction_max'))}")

    # Logements sociaux
    if d_rpls.get("nb_log"):
        section("LOGEMENTS SOCIAUX (RPLS)")
        print(f"  Nb logements sociaux     : {v(d_rpls.get('nb_log'))}")
        print(f"  Dans QPV                 : {v(d_rpls.get('dans_qpv'))}")
        print(f"  Étiquette énergie RPLS   : {v(d_rpls.get('classe_ener_principale'))}")
        print(f"  GES RPLS                 : {v(d_rpls.get('classe_ges_principale'))}")
        print(f"  Accessible PMR           : {v(d_rpls.get('accessible_pmr'))}")

    # Très Haut Débit
    if d_hthd.get("nb_pdl"):
        section("TRÈS HAUT DÉBIT (ARCEP)")
        print(f"  Nb points de livraison   : {v(d_hthd.get('nb_pdl'))}")
        print(f"  Types PDL                : {v(d_hthd.get('l_type_pdl'))}")

    # Urbanisme BDNB + GPU IGN
    section("URBANISME (BDNB + API Carto GPU IGN)")
    print(f"  Monument Historique      : {v(d_urba.get('monument_historique'))}")
    if d_urba.get("monument_historique"):
        print(f"  Dénomination MH          : {v(d_urba.get('denomination_monument_historique'))}")
        print(f"  Distance MH              : {v(d_urba.get('distance_monument_historique'))} m")
        print(f"  Source                   : {v(d_urba.get('source_monument_historique'))}")
    print(f"  Bâti patrimonial PLU     : {v(d_urba.get('zone_plu_bati_patrimonial'))}")
    print(f"  Contrainte urbanisme ac1 : {v(d_urba.get('contrainte_urbanisme_ac1'))}")
    # GPU (API Carto IGN)
    if d_gpu.get("plu_nb_zones", 0) > 0:
        print(f"  ── GPU IGN (Géoportail Urbanisme) ──")
        print(f"  Zone PLU                 : {v(d_gpu.get('plu_typezone'))} — {v(d_gpu.get('plu_libelle'))}")
        print(f"  Destination dominante    : {v(d_gpu.get('plu_destdomi'))}")
        print(f"  Prescriptions            : {v(d_gpu.get('plu_prescriptions_nb'))} — {v(d_gpu.get('plu_prescriptions'))}")
        print(f"  Servitudes SUP           : {v(d_gpu.get('sup_nb'))} — {v(d_gpu.get('sup_libelles'))}")
        print(f"  Partition document       : {v(d_gpu.get('plu_partition'))}")
    else:
        print(f"  GPU IGN                  : aucune donnée PLU (commune hors coverage ou RNU)")

    # Géorisques
    section("RISQUES NATURELS & TECHNOLOGIQUES (Géorisques BRGM)")
    print(f"  Argile / RGA             : {v(d_geo.get('argile_alea'))} (code : {v(d_geo.get('argile_code'))})")
    print(f"  Radon                    : Classe {v(d_geo.get('radon_classe'))}  |  BDNB : {v(d_risque.get('alea_radon'))}")
    print(f"  Sismique                 : Zone {v(d_geo.get('sismique_zone'))} — {v(d_geo.get('sismique_lib'))}")
    print(f"  Inondation               : {v(d_geo.get('inondation_nb_zones'))} zone(s) — {v(d_geo.get('inondation_detail'))}")
    print(f"  Cavités souterraines     : {v(d_geo.get('cavites_nb'))} (rayon 500m)")
    if d_geo.get("cavites_nb", 0) > 0: print(f"    Types                : {v(d_geo.get('cavites_types'))}")
    print(f"  Catastrophes naturelles  : {v(d_geo.get('catnat_nb'))} événement(s) — {v(d_geo.get('catnat_types'))}")
    print(f"  ICPE (rayon 500m)        : {v(d_geo.get('icpe_rayon_500m'))} installation(s)")
    if d_geo.get("icpe_rayon_500m", 0) > 0: print(f"    Sites                : {v(d_geo.get('icpe_noms'))}")

    # France Chaleur Urbaine
    section("RÉSEAU DE CHALEUR (France Chaleur Urbaine)")
    if d_fcu:
        elig = d_fcu.get("fcu_eligible")
        print(f"  Éligible raccordement    : {'✓ OUI' if elig else '✗ Non'}")
        print(f"  Distance réseau          : {v(d_fcu.get('fcu_distance_m'))} m  |  Réseau : {v(d_fcu.get('fcu_reseau_nom'))}")
        print(f"  % EnR&R                  : {v(d_fcu.get('fcu_enr_pct'))} %  |  CO₂ : {v(d_fcu.get('fcu_co2'))} kg/MWh")
    else:
        print(f"  BDNB distance réseau     : {v(d_reseau.get('indicateur_distance_au_reseau'))}")
        print(f"  Réseau en construction   : {v(d_reseau.get('reseau_en_construction'))}")
        if d_reseau.get("id_reseau"): print(f"  ID réseau BDNB           : {v(d_reseau.get('id_reseau'))}")

    # Benchmark ADEME
    if d_bench.get("commune") or d_bench.get("activite"):
        section("BENCHMARK TERTIAIRE (ADEME — slugs corrigés v2.1)")
        if d_bench.get("commune"):
            cm = d_bench["commune"]
            print(f"  Commune {str(cm.get('libelle_commune','')):<22} : {v(cm.get('conso_m2_kwh'))} kWh/m²")
        if d_bench.get("activite"):
            print(f"  Benchmark par activité :")
            for act in d_bench["activite"][:3]:
                print(f"    {str(act.get('libelle_activite','?')):<32} : médiane {v(act.get('conso_m2_kwh_mediane'))} kWh/m²")

    # SIRENE
    section(f"ÉTABLISSEMENTS SIRENE À PROXIMITÉ ({len(d_sirene)} trouvé(s))")
    if d_sirene:
        for e in d_sirene:
            print(f"  {e.get('nom','—')} | SIRET: {e.get('siret','—')} | NAF: {e.get('naf_libelle','—')} | Effectif: {e.get('effectif','—')}")
    else: print("  Aucun dans le rayon.")

    # Annuaire Éducation
    if d_edu:
        section(f"ÉTABLISSEMENTS SCOLAIRES À PROXIMITÉ ({len(d_edu)} trouvé(s))")
        for e in d_edu:
            print(f"  {e.get('nom_etablissement','—')} ({e.get('type_etablissement','—')}) — UAI: {e.get('identifiant_de_l_etablissement','—')}")
            print(f"    {e.get('adresse_1','—')}, {e.get('code_postal','—')} {e.get('nom_commune','—')} | {e.get('statut_public_prive','—')} | {e.get('nombre_d_eleves','—')} élèves")

    # DPE ADEME open data
    def champ(d, *cles):
        for k in cles:
            val = d.get(k)
            if val and val not in (None, "", "None", "nan"): return str(val)
        return "—"
    if donnees_ademe:
        section(f"DPE ADEME OPEN DATA ({len(donnees_ademe)} résultat(s))")
        for d in donnees_ademe[:3]:
            print(f"  N° DPE : {champ(d,'numero_dpe','Numero_DPE')}  |  "
                  f"Énergie : {champ(d,'classe_consommation_energie','Etiquette_DPE')}  "
                  f"GES : {champ(d,'classe_estimation_ges','Etiquette_GES')}  "
                  f"Secteur : {champ(d,'secteur_activite','Secteur_activite_principale_batiment')}")

    # API Entreprise DINUM (stub — habilitation requise)
    section("COUCHE FINANCIÈRE PROPRIÉTAIRE (API Entreprise DINUM)")
    siren = d_prop.get("siren")
    if siren:
        print(f"  SIREN propriétaire       : {v(siren)}")
        print(f"  ⚠️  API Entreprise requiert une habilitation administrative.")
        print(f"  Données disponibles sur habilitation :")
        print(f"    • Liasses fiscales DGFIP  : CA, bilans 3 dernières années")
        print(f"    • Certification Qualibat  : prestataires travaux RGE sur site")
        print(f"    • Attestation Urssaf      : vigilance sous-traitants")
        print(f"    • Effectifs mensuel GIP MDS")
        print(f"  → Demande habilitation : https://entreprise.api.gouv.fr/cas-usage/batiment")
    else:
        print("  SIREN non disponible pour ce bâtiment (données propriétaire limitées open data).")

    # Rayon X BDNB
    titre("RAYON X — DONNÉES BRUTES BDNB (tous champs non nuls)")
    all_data = {**d_base, **d_usage, **d_ffo, **d_topo, **d_dpe, **d_risque, **d_reseau,
                **d_urba, **d_geospx, **d_dvf_stat, **d_dvf_repr}
    EXCLURE = {"geom_groupe", "geom_groupe_pos_wgs84", "geom_cstr", "geom_adresse", "geom_iris"}
    for key, value in sorted(all_data.items()):
        if key not in EXCLURE and value not in (None, "", "None", [], {}):
            print(f"  {key:<45} : {v(value)}")

    # DPE complet XLS
    print(f"\n  [6/6] Récupération DPE complet (Observatoire ADEME)...")
    dossier_script = os.path.dirname(os.path.abspath(__file__))
    donnees_dpe_complet = telecharger_dossier_complet(numero_dpe, dossier_script)

    # Export JSON
    slug = slugifier(adresse_affichee)
    ts   = datetime.now().strftime("%Y%m%d_%H%M")
    chemin_json = os.path.join(dossier_script, f"akila_{slug}_{ts}.json")
    export = {
        "adresse": adresse_affichee, "resolution": res,
        "bdnb": {
            "base": d_base, "usage": d_usage, "ffo": d_ffo, "topo": d_topo,
            "proprietaire": d_prop, "dpe": d_dpe, "dpe_complet": d_dpe_complet,
            "risques": d_risque, "reseau": d_reseau,
            "elec": d_elec, "gaz": d_gaz, "reseaux_chaleur": d_reseaux,
            "sitadel": d_sitadel,
            "dvf_statistique": d_dvf_stat, "dvf_representatif": d_dvf_repr,
            "urbanisme": d_urba, "bpe": d_bpe, "rnc": d_rnc, "rpls": d_rpls,
            "qpv": d_qpv, "hthd": d_hthd, "geospx": d_geospx,
            "zoac": d_zoac, "equ": d_equ,
        },
        "georisques": d_geo,
        "gpu_urbanisme": d_gpu,
        "france_chaleur": d_fcu,
        "sirene": d_sirene,
        "education": d_edu,
        "benchmark_ademe": d_bench,
        "ademe_open": donnees_ademe,
        "dpe_complet_xlsx": donnees_dpe_complet if isinstance(donnees_dpe_complet, dict) else {},
        "genere_le": datetime.now().isoformat()
    }
    try:
        with open(chemin_json, "w", encoding="utf-8") as f:
            json.dump(export, f, ensure_ascii=False, indent=2, default=str)
        print(f"\n  Export JSON → {chemin_json}")
    except Exception as e:
        print(f"\n  Export JSON échoué : {e}")

    print(f"\n{SEP2}\n")


if __name__ == "__main__":
    try: main()
    except KeyboardInterrupt: sys.exit(0)