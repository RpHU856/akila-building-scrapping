"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  AKILA_prospect_v2.py — Scanner Full-Stack Bâtiment                        ║
║                                                                              ║
║  Entrée   : RNB / Clé BAN / Adresse libre                                  ║
║  Sortie   : Terminal structuré + JSON automatique + XLS DPE complet         ║
║                                                                              ║
║  SOURCES INTÉGRÉES (15 APIs / datasets)                                     ║
║  ── Identité & Géométrie ──────────────────────────────────────────────     ║
║    • RNB        Registre National des Bâtiments (rnb_id, statut, géom)     ║
║    • BAN        Base Adresse Nationale (géocodage, cle_interop)             ║
║    • BDNB       Base Données Nationale Bâtiments (CSTB) — 30+ tables       ║
║  ── Énergie & DPE ─────────────────────────────────────────────────────     ║
║    • ADEME      DPE Logements existants / neufs / tertiaire (open data)    ║
║    • ADEME      Observatoire DPE-Audit (XLS complet 475 variables)         ║
║    • ADEME      Conso tertiaire par activité (benchmark kWh/m²)            ║
║    • ADEME      Conso tertiaire par commune (comparaison territoriale)      ║
║    • ADEME      Conso par vecteur énergétique (gaz/élec/chaleur)           ║
║  ── Risques & Environnement ───────────────────────────────────────────     ║
║    • Géorisques API v1 — argile, radon, sismique, inondation, ICPE, catnat ║
║  ── Réseaux de chaleur ────────────────────────────────────────────────     ║
║    • France Chaleur Urbaine — éligibilité + distance réseau par GPS        ║
║  ── Occupation & Usage ────────────────────────────────────────────────     ║
║    • SIRENE BAN  Entreprises géolocalisées sur la parcelle (SIRET, NAF)    ║
║    • Annuaire Éducation Nationale (lycées/établissements scolaires)        ║
║  ── Transactions & Foncier ────────────────────────────────────────────     ║
║    • DVF         Demandes de Valeurs Foncières géolocalisées               ║
║    • SITADEL     Permis de construire (via BDNB)                           ║
║                                                                              ║
║  Prérequis : pip install requests pandas openpyxl                           ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import requests, json, sys, ast, os, re
from datetime import datetime
import unicodedata

# ── URLS ──────────────────────────────────────────────────────────────────────
RNB_BASE        = "https://rnb-api.beta.gouv.fr/api/alpha/buildings"
BAN_URL         = "https://api-adresse.data.gouv.fr/search/"
BDNB_URL        = "https://api.bdnb.io/v1/bdnb/donnees"
ADEME_TERT_OLD  = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe-tertiaire/lines"
ADEME_TERT_NEW  = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe01tertiaire/lines"
ADEME_LOG_OLD   = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe-france/lines"
ADEME_LOG_EXIST = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe03existant/lines"
ADEME_LOG_NEUF  = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe02neuf/lines"
OBS_BASE        = "https://observatoire-dpe-audit.ademe.fr"
GEORISQUES_BASE = "https://georisques.gouv.fr/api/v1"
FCU_BASE        = "https://france-chaleur-urbaine.beta.gouv.fr/api/v1"
SIRENE_BASE     = "https://recherche-entreprises.api.gouv.fr/search"
EDU_URL         = "https://data.education.gouv.fr/api/explore/v2.1/catalog/datasets/fr-en-annuaire-education/records"
DVF_URL         = "https://apicarto.ign.fr/api/dvf/mutation"
ADEME_CONSO_ACT = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe-conso-tertiaire-par-activite/lines"
ADEME_CONSO_COM = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe-conso-tertiaire-par-commune/lines"

HEADERS = {"User-Agent": "AKILA-Prospect/2.0", "Accept": "application/json"}
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


# ─────────────────────────────────────────────────────────────────────────────
# RÉSOLUTION ENTRÉE (RNB / BAN / Adresse)
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
        data = get(f"{RNB_BASE}/address/", {"cle_interop": entree, "limit": 1})
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
# NOUVEAU — GÉORISQUES (risques complets par GPS / INSEE)
# ─────────────────────────────────────────────────────────────────────────────
def collecter_georisques(lat, lon, code_insee=None):
    if not lat or not lon: return {}
    result = {}

    # Argile (précision parcelle)
    d = get(f"{GEORISQUES_BASE}/argiles", {"latlon": f"{lon},{lat}", "rayon": 100})
    if d and d.get("data"):
        r = d["data"][0]
        result["argile_alea"] = r.get("lib_risque_jo", r.get("code_alea", "—"))
        result["argile_code"] = r.get("code_alea", "—")

    # Sismique (précision commune)
    d = get(f"{GEORISQUES_BASE}/zonage-sismique", {"latlon": f"{lon},{lat}", "rayon": 100})
    if d and d.get("data"):
        r = d["data"][0]
        result["sismique_zone"] = r.get("zone", r.get("code_zone", "—"))
        result["sismique_lib"]  = r.get("lib_zone", "—")

    # Radon (précision commune)
    if code_insee:
        d = get(f"{GEORISQUES_BASE}/radon", {"code_insee": code_insee})
        if d and d.get("data"):
            result["radon_classe"] = d["data"][0].get("classe_potentiel", "—")

    # Inondation AZI
    d = get(f"{GEORISQUES_BASE}/azi", {"latlon": f"{lon},{lat}", "rayon": 200})
    if d and d.get("data"):
        result["inondation_nb_zones"] = len(d["data"])
        result["inondation_detail"]   = ", ".join(r.get("lib_type_alea", r.get("typeAlea", "")) for r in d["data"][:3]) or "—"
    else:
        result["inondation_nb_zones"] = 0
        result["inondation_detail"]   = "Aucune zone"

    # Cavités souterraines
    d = get(f"{GEORISQUES_BASE}/cavites", {"latlon": f"{lon},{lat}", "rayon": 500})
    if d and d.get("data"):
        result["cavites_nb"]    = len(d["data"])
        result["cavites_types"] = ", ".join(set(r.get("typeCavite", "") for r in d["data"][:5])) or "—"
    else:
        result["cavites_nb"] = 0

    # Catastrophes naturelles (commune)
    if code_insee:
        d = get(f"{GEORISQUES_BASE}/gaspar/catnat", {"code_insee_commune": code_insee, "page": 1, "page_size": 5})
        if d and d.get("data"):
            result["catnat_nb"]      = d.get("total", len(d["data"]))
            result["catnat_types"]   = ", ".join(set(r.get("libDomCatNat", "") for r in d["data"][:5])) or "—"
            result["catnat_derniere"] = d["data"][0].get("datFin", d["data"][0].get("dateDeb", "—"))
        else:
            result["catnat_nb"] = 0

    # ICPE dans 500m
    d = get(f"{GEORISQUES_BASE}/installations-classees", {"latlon": f"{lon},{lat}", "rayon": 500})
    if d and d.get("data"):
        result["icpe_rayon_500m"] = len(d["data"])
        result["icpe_noms"]       = ", ".join(r.get("raisonSociale", r.get("nomEtab", "")) for r in d["data"][:3]) or "—"
    else:
        result["icpe_rayon_500m"] = 0

    return result


# ─────────────────────────────────────────────────────────────────────────────
# NOUVEAU — FRANCE CHALEUR URBAINE
# ─────────────────────────────────────────────────────────────────────────────
def collecter_fcu(lat, lon):
    if not lat or not lon: return {}
    d = get(f"{FCU_BASE}/eligibility", {"lat": lat, "lon": lon})
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
# NOUVEAU — SIRENE : occupants du bâtiment
# ─────────────────────────────────────────────────────────────────────────────
def collecter_sirene(lat, lon, rayon_m=80):
    if not lat or not lon: return []
    d = get(SIRENE_BASE, {"lat": lat, "long": lon, "radius": rayon_m / 1000, "per_page": 5})
    if not d or not d.get("results"): return []
    result = []
    for e in d["results"][:5]:
        result.append({
            "siret":       e.get("siret", "—"),
            "nom":         e.get("nom_complet", e.get("nom_raison_sociale", "—")),
            "naf_code":    e.get("activite_principale", "—"),
            "naf_libelle": e.get("libelle_activite_principale", "—"),
            "adresse":     e.get("adresse", "—"),
            "effectif":    e.get("tranche_effectif_salarie", "—"),
            "statut":      e.get("etat_administratif", "—"),
        })
    return result


# ─────────────────────────────────────────────────────────────────────────────
# NOUVEAU — ANNUAIRE ÉDUCATION NATIONALE
# ─────────────────────────────────────────────────────────────────────────────
def collecter_education(lat, lon, rayon_m=300):
    if not lat or not lon: return []
    d = get(EDU_URL, {
        "where": f"within_distance(coordonnees_gps, geom'POINT({lon} {lat})', {rayon_m}m)",
        "limit": 5,
        "select": "identifiant_de_l_etablissement,nom_etablissement,type_etablissement,adresse_1,code_postal,nom_commune,telephone,statut_public_prive,nombre_d_eleves"
    })
    if not d or not d.get("results"): return []
    return d["results"]


# ─────────────────────────────────────────────────────────────────────────────
# NOUVEAU — DVF : transactions foncières
# ─────────────────────────────────────────────────────────────────────────────
def collecter_dvf(lat, lon, rayon_m=150):
    if not lat or not lon: return []
    d = get(DVF_URL, {"lon": lon, "lat": lat, "dist": rayon_m, "limit": 5})
    if d and d.get("features"):
        return [f.get("properties", {}) for f in d["features"][:5]]
    return []


# ─────────────────────────────────────────────────────────────────────────────
# NOUVEAU — BENCHMARK TERTIAIRE ADEME
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
# ADEME DPE OPEN DATA (inchangé)
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
# OBSERVATOIRE DPE — XLS complet (5 feuilles / 475 variables)
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
    print(f"  Tentative téléchargement XLS Observatoire...")

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
    print(f"    Déposez {numero_dpe}.xlsx dans le dossier du script.")
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
                if pd.notna(k) and pd.notna(val):
                    try: d[str(k).strip()] = float(val) if str(val).replace(".", "").replace("-", "").isdigit() else str(val).strip()
                    except Exception: d[str(k).strip()] = str(val).strip()
            res[feuille] = d

    if "rapport" in xl:
        df = xl["rapport"].dropna(how="all")
        import pandas as _pd
        rapport = {"descriptif_simplifie": [], "packs_travaux": [], "gestes_entretien": []}
        pack = None
        for _, row in df.iterrows():
            k   = str(row.iloc[0]).strip() if _pd.notna(row.iloc[0]) else ""
            val = str(row.iloc[1]).strip() if _pd.notna(row.iloc[1]) else ""
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
    print(f"  Moteur    : {admin.get('version_moteur_calcul','—')}")

    section("CARACTÉRISTIQUES (XLS complet)")
    print(f"  Période construction : {logem.get('periode_construction','—')}")
    print(f"  Surface habitable    : {logem.get('surface_habitable_logement','—')} m²")
    print(f"  Zone climatique      : {logem.get('zone_climatique','—')}  |  Inertie : {logem.get('classe_inertie','—')}")
    print(f"  Méthode DPE          : {logem.get('methode_application_dpe_log','—')}")

    etiq_e = sortie.get("classe_bilan_dpe","—")
    etiq_g = sortie.get("classe_emission_ges","—")
    conso  = sortie.get("conso_5_usages_m2") or sortie.get("ep_conso_5_usages_m2","—")
    ges    = sortie.get("emission_ges_5_usages_m2","—")
    try: cout = f"{round(float(sortie.get('cout_5_usages',0))):,}".replace(",", " ") + " €/an"
    except Exception: cout = "—"
    section("RÉSULTATS 3CL")
    print(f"  Étiquette Énergie : {etiq_e}  ({conso} kWh EP/m²/an)")
    print(f"  Étiquette GES     : {etiq_g}  ({ges} kg CO₂/m²/an)")
    print(f"  Coût annuel       : {cout}  |  Type énergie : {sortie.get('type_energie','—')}")
    print(f"  Confort été       : {sortie.get('indicateur_confort_ete','—')}  |  Ubat : {sortie.get('ubat','—')} W/m².K")

    section("DÉPERDITIONS THERMIQUES")
    total = sortie.get("deperdition_enveloppe", 0)
    for label, key in [("Murs","deperdition_mur"),("Plancher haut","deperdition_plancher_haut"),
                        ("Plancher bas","deperdition_plancher_bas"),("Baies vitrées","deperdition_baie_vitree"),
                        ("Ponts therm.","deperdition_pont_thermique"),("Renouvl. air","deperdition_renouvellement_air"),
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
        if donnees:
            afficher_dpe_complet(donnees)
            return donnees

    # Fallback API simplifiée
    print(f"\n  Fallback → API simplifiée data.ademe.fr")
    print(f"  {'─'*40}")
    for nom, url in [("Logements post-2021", ADEME_LOG_EXIST),("Tertiaire post-2021", ADEME_TERT_NEW),
                      ("Logements neufs", ADEME_LOG_NEUF),("Tertiaire avant", ADEME_TERT_OLD),("Logements avant", ADEME_LOG_OLD)]:
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
                        print(f"  Étiquette   : {c('classe_consommation_energie','Etiquette_DPE','classe_bilan_dpe')}")
                        print(f"  GES         : {c('classe_estimation_ges','Etiquette_GES','classe_emission_ges')}")
                        print(f"  Conso EP    : {c('consommation_energie','Conso_5_usages_ep_m2','conso_5_usages_ep_m2')} kWh/m²/an")
                        chemin_json = os.path.join(dossier_sortie, f"DPE_{numero_dpe}.json")
                        with open(chemin_json, "w", encoding="utf-8") as f:
                            json.dump(result, f, indent=2, ensure_ascii=False)
                        return result
            except Exception: continue
    print(f"  ✗ DPE non trouvé. → {OBS_BASE}/pub/dpe/{numero_dpe}")
    return {}


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    titre("AKILA PROSPECT v2 — SCANNER FULL-STACK BÂTIMENT")
    print(f"  Sources : RNB · BAN · BDNB · Géorisques · FCU · SIRENE · Éducation · DVF · ADEME")

    choix = input("\n  Cible : [T]ertiaire ou [P]articulier ? [T/p] : ").strip().upper()
    type_cible = "P" if choix == "P" else "T"
    hint = None
    if type_cible == "T":
        hint = input("  Type d'activité pour benchmark (ex: lycée / bureau / vide) : ").strip() or None

    entree = input("  Identifiant RNB, clé BAN ou adresse : ").strip()
    if not entree: return

    print(f"\n  [1/6] Résolution adresse (BAN + RNB)...")
    res = resoudre_entree(entree)
    bat_id = res.get("bat_id_bdnb")
    if not bat_id:
        print("\n  ✗ Bâtiment introuvable dans la BDNB.")
        print("    Vérifiez l'adresse ou essayez avec la clé BAN directe.")
        return

    lat, lon = res.get("lat"), res.get("lon")
    code_insee = res.get("code_commune_insee")

    print(f"  [2/6] BDNB — extraction complète ({bat_id})...")
    p1 = {"batiment_groupe_id": f"eq.{bat_id}", "limit": 1}
    pm = {"batiment_groupe_id": f"eq.{bat_id}", "order": "millesime.desc", "limit": 4}
    first = lambda lst: lst[0] if lst else {}

    d_base    = first(bdnb("batiment_groupe", {**p1, "select": "code_commune_insee,libelle_commune_insee,s_geom_groupe,code_iris"}))
    d_usage   = first(bdnb("batiment_groupe_synthese_propriete_usage", {**p1, "select": "usage_principal_bdnb_open,categorie_usage_propriete"}))
    d_ffo     = first(bdnb("batiment_groupe_ffo_bat", {**p1, "select": "annee_construction,mat_mur_txt,mat_toit_txt,nb_log,nb_niveau,usage_niveau_1_txt"}))
    d_topo    = first(bdnb("batiment_groupe_bdtopo_bat", {**p1, "select": "hauteur_mean,altitude_sol_mean,l_usage_1,nb_etages"}))
    d_prop    = first(bdnb("batiment_groupe_proprietaire", {**p1, "select": "bat_prop_denomination_proprietaire,bat_prop_type_proprietaire"}))
    d_risque  = first(bdnb("batiment_groupe_risques", {**p1, "select": "alea_argile,alea_radon,alea_sismique"}))
    d_reseau  = first(bdnb("batiment_groupe_indicateur_reseau_chaud_froid", {**p1, "select": "indicateur_distance_au_reseau,reseau_en_construction,identifiant_reseau"}))
    d_sitadel = bdnb("sitadel", {**p1, "select": "date_autorisation,type_autorisation,libelle_destination_principale", "limit": 5}) or []

    if type_cible == "T":
        d_dpe = first(bdnb("batiment_groupe_dpe_tertiaire", {**p1, "select": "identifiant_dpe,classe_conso_energie_dpe_tertiaire,classe_emission_ges_dpe_tertiaire,conso_dpe_tertiaire_ep_m2,emission_ges_dpe_tertiaire_m2,type_energie_chauffage,date_etablissement_dpe,surface_utile,shon"}))
    else:
        d_dpe = first(bdnb("batiment_groupe_dpe_representatif_logement", {**p1, "select": "identifiant_dpe,classe_bilan_dpe,classe_emission_ges,conso_5_usages_ep_m2,emission_ges_5_usages_m2,type_energie_chauffage,date_etablissement_dpe,surface_habitable_immeuble"}))

    d_elec = bdnb("batiment_groupe_dle_elec_multimillesime", {**pm, "select": "millesime,conso_tot,nb_pdl_tot"})
    d_gaz  = bdnb("batiment_groupe_dle_gaz_multimillesime",  {**pm, "select": "millesime,conso_tot,nb_pdl_tot"})
    numero_dpe = v(d_dpe.get("identifiant_dpe"))

    print(f"  [3/6] Géorisques + France Chaleur Urbaine...")
    d_geo = collecter_georisques(lat, lon, code_insee)
    d_fcu = collecter_fcu(lat, lon)

    print(f"  [4/6] SIRENE + Annuaire Éducation + DVF...")
    d_sirene = collecter_sirene(lat, lon, rayon_m=80)
    d_edu    = collecter_education(lat, lon, rayon_m=300)
    d_dvf    = collecter_dvf(lat, lon, rayon_m=150)

    print(f"  [5/6] Benchmark tertiaire ADEME + DPE open data...")
    d_bench = collecter_benchmark_tertiaire(code_insee or d_base.get("code_commune_insee"), hint)
    donnees_ademe, erreurs_ademe = collecter_ademe_direct(res.get("rnb_id"), res.get("adresse_label"), res.get("cle_ban"), type_cible, numero_dpe if numero_dpe != "—" else None)

    # ── AFFICHAGE ──────────────────────────────────────────────────────────────
    adresse_affichee = res.get("adresse_label") or "Adresse non résolue"
    titre(f"CARTE D'IDENTITÉ : {adresse_affichee}")

    section("IDENTIFIANTS OFFICIELS")
    print(f"  ID RNB (Bâtiment)        : {v(res.get('rnb_id'))}")
    print(f"  ID BDNB (CSTB)           : {v(bat_id)}")
    print(f"  Clé BAN (Interop)        : {v(res.get('cle_ban'))}")
    print(f"  Code INSEE commune       : {v(code_insee)}")
    if res.get("rnb_id"): print(f"  Fiche RNB                : https://rnb.beta.gouv.fr/batiment/{res['rnb_id']}")
    print(f"  Fiche BDNB               : https://bdnb.io/batiment/{bat_id}")

    section("LOCALISATION")
    print(f"  Commune                  : {v(d_base.get('code_commune_insee'))} — {v(d_base.get('libelle_commune_insee'))}")
    print(f"  Code IRIS                : {v(d_base.get('code_iris'))}")
    print(f"  Coordonnées GPS          : {v(lat)}, {v(lon)}")

    section("CARACTÉRISTIQUES PHYSIQUES")
    print(f"  Année de construction    : {v(d_ffo.get('annee_construction'))}")
    try:
        print(f"  Âge du bâtiment          : {2025 - int(d_ffo['annee_construction'])} ans")
    except Exception: pass
    surf = d_base.get("s_geom_groupe")
    print(f"  Surface emprise au sol   : {v(round(float(surf)) if surf else None, 'm²')}")
    print(f"  Hauteur moyenne          : {v(d_topo.get('hauteur_mean'), 'm')}")
    print(f"  Nb niveaux               : {v(d_topo.get('nb_etages') or d_ffo.get('nb_niveau'))}")
    print(f"  Matériaux murs           : {v(d_ffo.get('mat_mur_txt'))}")
    print(f"  Matériaux toit           : {v(d_ffo.get('mat_toit_txt'))}")
    print(f"  Usages BD TOPO           : {v(d_topo.get('l_usage_1'))}")

    section("USAGE & PROPRIÉTÉ")
    print(f"  Usage principal (BDNB)   : {v(d_usage.get('usage_principal_bdnb_open'))}")
    print(f"  Catégorie usage/prop.    : {v(d_usage.get('categorie_usage_propriete'))}")
    print(f"  Usage foncier (FF)       : {v(d_ffo.get('usage_niveau_1_txt'))}")
    prop = d_prop.get("bat_prop_denomination_proprietaire") or d_prop.get("l_denomination_proprietaire")
    print(f"  Propriétaire             : {v(prop)}")
    print(f"  Type propriétaire        : {v(d_prop.get('bat_prop_type_proprietaire'))}")

    section("PERFORMANCE ÉNERGÉTIQUE (BDNB)")
    print(f"  Numéro DPE               : {numero_dpe}")
    if type_cible == "T":
        print(f"  Étiquette Énergie        : {v(d_dpe.get('classe_conso_energie_dpe_tertiaire'))}")
        print(f"  Étiquette GES            : {v(d_dpe.get('classe_emission_ges_dpe_tertiaire'))}")
        print(f"  Consommation EP          : {v(d_dpe.get('conso_dpe_tertiaire_ep_m2'), 'kWh/m²/an')}")
        print(f"  Émissions GES            : {v(d_dpe.get('emission_ges_dpe_tertiaire_m2'), 'kg CO2/m²/an')}")
        print(f"  Énergie chauffage        : {v(d_dpe.get('type_energie_chauffage'))}")
        print(f"  Surface utile / SHON     : {v(d_dpe.get('surface_utile'))} m² / {v(d_dpe.get('shon'))} m²")
    else:
        print(f"  Étiquette Énergie        : {v(d_dpe.get('classe_bilan_dpe'))}")
        print(f"  Étiquette GES            : {v(d_dpe.get('classe_emission_ges'))}")
        print(f"  Conso 5 usages EP        : {v(d_dpe.get('conso_5_usages_ep_m2'), 'kWh/m²/an')}")
        print(f"  Émissions GES            : {v(d_dpe.get('emission_ges_5_usages_m2'), 'kg CO2/m²/an')}")
        print(f"  Surface habitable        : {v(d_dpe.get('surface_habitable_immeuble'), 'm²')}")
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
            print(f"    {e.get('millesime','—')}  →  {round(float(c)):,} kWh  ({v(e.get('nb_pdl_tot'))} PDL)  {cout}".replace(",", " "))
        if len(vals) >= 2:
            delta = (vals[0]-vals[-1])/vals[-1]*100 if vals[-1] else 0
            print(f"    Tendance : {'↓' if delta<0 else '↑'} {abs(round(delta,1))}%")
    else: print("    Non disponible.")
    print(f"  Gaz :")
    if d_gaz:
        for g in d_gaz:
            c = g.get("conso_tot")
            try: print(f"    {g.get('millesime','—')}  →  {round(float(c)):,} kWh".replace(",", " "))
            except Exception: print(f"    {g.get('millesime','—')}  →  —")
    else: print("    Non disponible.")

    # Géorisques
    section("RISQUES NATURELS & TECHNOLOGIQUES (API Géorisques BRGM)")
    print(f"  Argile / RGA             : {v(d_geo.get('argile_alea'))} (code : {v(d_geo.get('argile_code'))})")
    print(f"  Radon                    : Classe {v(d_geo.get('radon_classe'))}  |  BDNB : {v(d_risque.get('alea_radon'))}")
    print(f"  Sismique                 : Zone {v(d_geo.get('sismique_zone'))} — {v(d_geo.get('sismique_lib'))}")
    print(f"  Inondation               : {v(d_geo.get('inondation_nb_zones'))} zone(s) — {v(d_geo.get('inondation_detail'))}")
    print(f"  Cavités souterraines     : {v(d_geo.get('cavites_nb'))} (rayon 500m)  {('types : ' + v(d_geo.get('cavites_types'))) if d_geo.get('cavites_nb',0)>0 else ''}")
    print(f"  Catastrophes naturelles  : {v(d_geo.get('catnat_nb'))} événement(s) — {v(d_geo.get('catnat_types'))}")
    print(f"  ICPE (rayon 500m)        : {v(d_geo.get('icpe_rayon_500m'))} installation(s)  {('→ ' + v(d_geo.get('icpe_noms'))) if d_geo.get('icpe_rayon_500m',0)>0 else ''}")

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

    # Benchmark ADEME
    if d_bench.get("commune") or d_bench.get("activite"):
        section("BENCHMARK TERTIAIRE (ADEME)")
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
    else: print("  Aucun établissement dans le rayon.")

    # Annuaire Éducation
    if d_edu:
        section(f"ÉTABLISSEMENTS SCOLAIRES À PROXIMITÉ ({len(d_edu)} trouvé(s))")
        for e in d_edu:
            print(f"  {e.get('nom_etablissement','—')} ({e.get('type_etablissement','—')}) — UAI: {e.get('identifiant_de_l_etablissement','—')}")
            print(f"    {e.get('adresse_1','—')}, {e.get('code_postal','—')} {e.get('nom_commune','—')} | {e.get('statut_public_prive','—')} | {e.get('nombre_d_eleves','—')} élèves")

    # DVF
    if d_dvf:
        section(f"TRANSACTIONS FONCIÈRES DVF À PROXIMITÉ ({len(d_dvf)} trouvée(s))")
        for mut in d_dvf[:3]:
            date  = mut.get("date_mutation", "—")
            prix  = mut.get("valeur_fonciere", "—")
            surf  = mut.get("surface_reelle_bati", "—")
            type_ = mut.get("type_local", "—")
            try: prix_fmt = f"{int(float(str(prix).replace(',','.'))):,}".replace(",", " ") + " €"
            except Exception: prix_fmt = str(prix)
            print(f"  {date} — {type_} — {surf} m² — {prix_fmt}")

    # DPE ADEME open data
    def champ(d, *cles):
        for k in cles:
            val = d.get(k)
            if val and val not in (None, "", "None", "nan"): return str(val)
        return "—"
    if donnees_ademe:
        section(f"DPE ADEME OPEN DATA ({len(donnees_ademe)} résultat(s))")
        for d in donnees_ademe[:3]:
            ndpe   = champ(d, "numero_dpe", "Numero_DPE")
            etiq_e = champ(d, "classe_consommation_energie", "Etiquette_DPE")
            etiq_g = champ(d, "classe_estimation_ges", "Etiquette_GES")
            secteur = champ(d, "secteur_activite", "Secteur_activite_principale_batiment", "Type_batiment")
            print(f"  N° DPE : {ndpe}  |  Énergie : {etiq_e}  GES : {etiq_g}  Secteur : {secteur}")

    # SITADEL
    if d_sitadel:
        section(f"PERMIS DE CONSTRUIRE SITADEL ({len(d_sitadel)} enregistrement(s))")
        for pc in d_sitadel:
            print(f"  {pc.get('date_autorisation','—')} — {pc.get('type_autorisation','—')} — {pc.get('libelle_destination_principale','—')}")

    # Rayon X BDNB
    titre("RAYON X — DONNÉES BRUTES BDNB (tous champs non nuls)")
    all_data = {**d_base, **d_usage, **d_ffo, **d_topo, **d_dpe, **d_risque, **d_reseau}
    EXCLURE = {"geom_groupe", "geom_groupe_pos_wgs84", "geom_cstr", "geom_adresse"}
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
        "bdnb": {"base": d_base, "usage": d_usage, "ffo": d_ffo, "topo": d_topo,
                  "dpe": d_dpe, "risques": d_risque, "reseau": d_reseau,
                  "elec": d_elec, "gaz": d_gaz, "sitadel": d_sitadel},
        "georisques": d_geo, "france_chaleur": d_fcu,
        "sirene": d_sirene, "education": d_edu, "dvf": d_dvf,
        "benchmark_ademe": d_bench, "ademe_open": donnees_ademe,
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
