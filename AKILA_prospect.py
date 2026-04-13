"""
╔══════════════════════════════════════════════════════════════════════╗
║  AKILA_prospect.py — Scanner Hybride (ID Card + Rayon X)            ║
║  Entrée  : RNB / Clé BAN / Adresse libre                           ║
║  Sortie  : Terminal structuré + JSON + XLS DPE complet             ║
║  Sources : RNB · BAN · BDNB · ADEME · Observatoire DPE             ║
║  Prérequis : pip install requests pandas openpyxl                   ║
╚══════════════════════════════════════════════════════════════════════╝
"""

import requests
import json
import sys
import ast
import os
import io
from datetime import datetime
import unicodedata

# ── URLS ──────────────────────────────────────────────────────────────────────
RNB_BASE   = "https://rnb-api.beta.gouv.fr/api/alpha/buildings"
BAN_URL    = "https://api-adresse.data.gouv.fr/search/"
BDNB_URL   = "https://api-bdnb.io/v1/bdnb/donnees"

# Bases ADEME Open Data — identifiants vérifiés sur data.ademe.fr
ADEME_TERT_OLD = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe-tertiaire/lines"
ADEME_TERT_NEW = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe01tertiaire/lines"
ADEME_LOG_OLD  = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe-france/lines"
ADEME_LOG_EXIST= "https://data.ademe.fr/data-fair/api/v1/datasets/dpe03existant/lines"
ADEME_LOG_NEUF = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe02neuf/lines"

# Observatoire DPE-Audit ADEME (accessible depuis IP française, bloqué VPN étranger)
OBS_BASE   = "https://observatoire-dpe-audit.ademe.fr"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json,text/html,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9",
}

SEP  = "─" * 70
SEP2 = "═" * 70

# ─────────────────────────────────────────────────────────────────────────────
# UTILITAIRES & RÉSOLUTION
# ─────────────────────────────────────────────────────────────────────────────
def get(url, params=None):
    try:
        r = requests.get(url, params=params or {}, headers=HEADERS, timeout=30)
        return r.json() if r.ok else None
    except Exception:
        return None

def bdnb(endpoint, params):
    return get(f"{BDNB_URL}/{endpoint}", params) or []

def detecter_type_entree(entree):
    e = entree.strip().upper()
    if len(e) == 14 and e.count("-") == 2:
        blocs = e.split("-")
        if all(len(b) == 4 and b.isalnum() for b in blocs):
            return "rnb"
    if len(e) == 12 and e.isalnum() and not e.isdigit():
        return "rnb"
    if e.count("_") == 2 and all(p.isdigit() for p in e.split("_")):
        return "ban"
    return "adresse"

def v(val, unit=""):
    if val in (None, "", "None", r"\N", "\\N", "nan", "NaN", [], {}):
        return "—"
    if isinstance(val, str) and val.startswith("[") and val.endswith("]"):
        try:
            val_list = ast.literal_eval(val)
            val = ", ".join(str(x) for x in val_list)
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

def resoudre_entree(entree):
    type_entree = detecter_type_entree(entree)
    res = {
        "entree_originale": entree, "type_entree": type_entree,
        "rnb_id": None, "cle_ban": None, "adresse_label": None,
        "bdnb_bat_construction_id": None, "bat_id_bdnb": None,
        "lat": None, "lon": None
    }

    def extraire_rnb(data):
        pt = data.get("point", {}).get("coordinates", [])
        if pt:
            res["lon"], res["lat"] = pt[0], pt[1]
        addrs = data.get("addresses", [])
        if addrs:
            a = addrs[0]
            res["adresse_label"] = f"{a.get('street_number','')} {a.get('street','')} {a.get('city_zipcode','')} {a.get('city_name','')}".strip()
            res["cle_ban"] = a.get("id")
        for ext in data.get("ext_ids", []):
            if ext.get("source") == "bdnb":
                res["bdnb_bat_construction_id"] = ext.get("id")

    if type_entree == "rnb":
        rnb_propre = f"{entree[:4]}-{entree[4:8]}-{entree[8:]}" if len(entree) == 12 else entree
        data = get(f"{RNB_BASE}/{rnb_propre}/", {"with_plots": 1})
        if not data:
            data = get(f"{RNB_BASE}/{entree}/", {"with_plots": 1})
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
        link = bdnb("batiment_construction", {
            "batiment_construction_id": f"eq.{res['bdnb_bat_construction_id']}",
            "select": "batiment_groupe_id", "limit": 1
        })
        if link:
            res["bat_id_bdnb"] = link[0].get("batiment_groupe_id")

    if not res["bat_id_bdnb"] and res["cle_ban"]:
        for table, champ in [
            ("batiment_groupe_adresse", "cle_interop_adr_principale_ban"),
            ("rel_batiment_groupe_adresse", "cle_interop_adr")
        ]:
            rows = bdnb(table, {champ: f"eq.{res['cle_ban']}", "select": "batiment_groupe_id", "limit": 1})
            if rows:
                res["bat_id_bdnb"] = rows[0].get("batiment_groupe_id")
                break

    return res

# ─────────────────────────────────────────────────────────────────────────────
# REQUÊTE DIRECTE ADEME OPEN DATA
# ─────────────────────────────────────────────────────────────────────────────
def collecter_ademe_direct(rnb_id, adresse_label, cle_ban, type_cible, numero_dpe_connu=None):
    resultats = []
    erreurs = set()

    urls_tert  = [ADEME_TERT_NEW, ADEME_TERT_OLD]
    urls_log   = [ADEME_LOG_EXIST, ADEME_LOG_NEUF, ADEME_LOG_OLD]
    urls_toutes = urls_tert + urls_log if type_cible == "T" else urls_log + urls_tert
    urls_ademe  = urls_tert if type_cible == "T" else urls_log

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
# NOUVEAU : TÉLÉCHARGEMENT XLS COMPLET DEPUIS L'OBSERVATOIRE
# ─────────────────────────────────────────────────────────────────────────────
def telecharger_xlsx_observatoire(numero_dpe, dossier_sortie=None):
    """
    Télécharge le fichier XLS complet (5 feuilles, 475 variables) depuis
    l'Observatoire DPE-Audit ADEME.

    Fonctionne depuis une IP française sans VPN.
    Bloqué par Cloudflare depuis les IP VPN étrangères (403).

    Stratégies tentées dans l'ordre :
      1. Scraping de la page pub/dpe/{numero_dpe} pour trouver le lien de téléchargement
      2. Patterns URL directs connus
      3. Fallback : URL observatoire à ouvrir manuellement
    """
    if not numero_dpe or numero_dpe in ("—", "None", None):
        return None

    if dossier_sortie is None:
        dossier_sortie = os.path.dirname(os.path.abspath(__file__))

    chemin_xlsx = os.path.join(dossier_sortie, f"{numero_dpe}.xlsx")

    # ── Si déjà téléchargé, on réutilise
    if os.path.exists(chemin_xlsx):
        print(f"  XLS déjà présent localement : {chemin_xlsx}")
        return chemin_xlsx

    session = requests.Session()
    session.headers.update(HEADERS)

    # ── Stratégie 1 : récupérer la page HTML et trouver le lien de téléchargement
    print(f"  Tentative téléchargement XLS depuis l'Observatoire...")
    try:
        page_url = f"{OBS_BASE}/pub/dpe/{numero_dpe}"
        r = session.get(page_url, timeout=15, allow_redirects=True)
        if r.ok and len(r.content) > 1000:
            # Chercher les liens de téléchargement dans le HTML
            import re
            html = r.text
            # Patterns courants pour les liens XLS/XLSX dans les apps gouvernementales
            patterns = [
                r'href=["\']([^"\']*' + re.escape(numero_dpe) + r'[^"\']*\.xlsx?)["\']',
                r'href=["\']([^"\']*download[^"\']*)["\']',
                r'href=["\']([^"\']*export[^"\']*)["\']',
                r'"url"\s*:\s*"([^"]*\.xlsx?)"',
                r'"downloadUrl"\s*:\s*"([^"]*)"',
            ]
            for pattern in patterns:
                matches = re.findall(pattern, html, re.IGNORECASE)
                for match in matches:
                    dl_url = match if match.startswith("http") else f"{OBS_BASE}{match}"
                    try:
                        r2 = session.get(dl_url, timeout=20, stream=True)
                        ct = r2.headers.get("content-type", "")
                        if r2.ok and ("spreadsheet" in ct or "excel" in ct or "octet-stream" in ct or len(r2.content) > 5000):
                            with open(chemin_xlsx, "wb") as f:
                                f.write(r2.content)
                            print(f"  ✓ XLS téléchargé ({len(r2.content)//1024} Ko) → {chemin_xlsx}")
                            return chemin_xlsx
                    except Exception:
                        continue
    except Exception as e:
        pass

    # ── Stratégie 2 : patterns URL directs connus
    patterns_url = [
        f"{OBS_BASE}/pub/dpe/{numero_dpe}/download",
        f"{OBS_BASE}/pub/dpe/{numero_dpe}/export.xlsx",
        f"{OBS_BASE}/pub/dpe/{numero_dpe}.xlsx",
        f"{OBS_BASE}/api/dpe/{numero_dpe}/download",
        f"{OBS_BASE}/api/v1/dpe/{numero_dpe}/export",
        f"{OBS_BASE}/exportation/{numero_dpe}.xlsx",
    ]
    for url in patterns_url:
        try:
            r = session.get(url, timeout=15, allow_redirects=True, stream=True)
            ct = r.headers.get("content-type", "")
            if r.ok and ("spreadsheet" in ct or "excel" in ct or "octet-stream" in ct):
                with open(chemin_xlsx, "wb") as f:
                    f.write(r.content)
                print(f"  ✓ XLS téléchargé ({len(r.content)//1024} Ko) → {chemin_xlsx}")
                return chemin_xlsx
        except Exception:
            continue

    # ── Fallback : Cloudflare bloque (VPN ou IP étrangère)
    print(f"  ✗ Observatoire inaccessible (Cloudflare / VPN / IP étrangère)")
    print(f"  → Téléchargez manuellement : {OBS_BASE}/pub/dpe/{numero_dpe}")
    print(f"    Puis déposez le fichier {numero_dpe}.xlsx dans le même dossier que ce script")
    print(f"    Le script le lira automatiquement au prochain lancement.")
    return None


# ─────────────────────────────────────────────────────────────────────────────
# NOUVEAU : ANALYSE XLS DPE COMPLET (5 FEUILLES, 475 VARIABLES)
# ─────────────────────────────────────────────────────────────────────────────
def analyser_dpe_xlsx(chemin_xlsx):
    """
    Lit et structure le fichier XLS complet de l'Observatoire DPE-Audit.
    Retourne un dict avec toutes les données extraites des 5 feuilles :
      - administratif  : identifiants, adresse, géocodage
      - logement       : caractéristiques physiques (268 lignes)
      - logement_sortie: résultats calcul 3CL (déperditions, étiquettes, coûts)
      - rapport        : recommandations travaux et packs
      - lexique        : dictionnaire des 475 variables
    """
    try:
        import pandas as pd
    except ImportError:
        print("  ✗ pandas non installé. Lancez : pip install pandas openpyxl")
        return {}

    if not chemin_xlsx or not os.path.exists(chemin_xlsx):
        return {}

    try:
        xl = pd.read_excel(chemin_xlsx, sheet_name=None, header=0)
    except Exception as e:
        print(f"  ✗ Lecture XLS impossible : {e}")
        return {}

    resultat = {}

    # ── Feuille : administratif
    if "administratif" in xl:
        df = xl["administratif"].dropna(how="all")
        admin = {}
        for _, row in df.iterrows():
            k, val = row.iloc[0], row.iloc[1]
            if pd.notna(k) and pd.notna(val):
                admin[str(k).strip()] = str(val).strip()
        resultat["administratif"] = admin

    # ── Feuille : logement (caractéristiques physiques)
    if "logement" in xl:
        df = xl["logement"].dropna(how="all")
        logement = {}
        for _, row in df.iterrows():
            k = row.iloc[0]
            val = row.iloc[1] if len(row) > 1 else None
            if pd.notna(k) and pd.notna(val):
                logement[str(k).strip()] = str(val).strip()
        resultat["logement"] = logement

    # ── Feuille : logement_sortie (résultats 3CL)
    if "logement_sortie" in xl:
        df = xl["logement_sortie"].dropna(how="all")
        sortie = {}
        for _, row in df.iterrows():
            k = row.iloc[0]
            val = row.iloc[1] if len(row) > 1 else None
            if pd.notna(k) and pd.notna(val):
                try:
                    sortie[str(k).strip()] = float(val) if str(val).replace(".", "").replace("-", "").isdigit() else str(val).strip()
                except Exception:
                    sortie[str(k).strip()] = str(val).strip()
        resultat["logement_sortie"] = sortie

    # ── Feuille : rapport (recommandations travaux)
    if "rapport" in xl:
        df = xl["rapport"].dropna(how="all")
        rapport = {"descriptif_simplifie": [], "packs_travaux": [], "gestes_entretien": []}
        pack_courant = None

        for _, row in df.iterrows():
            k = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ""
            val = str(row.iloc[1]).strip() if pd.notna(row.iloc[1]) else ""

            if "pack_travaux_" in k and val.isdigit():
                pass  # en-tête de pack
            elif k.startswith("num_pack_travaux"):
                pack_courant = {"numero": val, "travaux": []}
                rapport["packs_travaux"].append(pack_courant)
            elif k.startswith("conso_5_usages_apres_travaux") and pack_courant:
                pack_courant["conso_apres_kwhep_m2"] = val
            elif k.startswith("emission_ges_5_usages_apres_travaux") and pack_courant:
                pack_courant["ges_apres_kgco2_m2"] = val
            elif k.startswith("cout_pack_travaux_min") and pack_courant:
                pack_courant["cout_min_eur_m2"] = val
            elif k.startswith("cout_pack_travaux_max") and pack_courant:
                pack_courant["cout_max_eur_m2"] = val
            elif k.startswith("travaux_") and pack_courant and val:
                pack_courant["travaux"].append(val)
            elif k.startswith("descriptif_simplifie_") and val:
                rapport["descriptif_simplifie"].append(val)
            elif k.startswith("descriptif_geste_entretien_") and val:
                rapport["gestes_entretien"].append(val)

        resultat["rapport"] = rapport

    return resultat


# ─────────────────────────────────────────────────────────────────────────────
# NOUVEAU : AFFICHAGE ENRICHI DU DPE COMPLET
# ─────────────────────────────────────────────────────────────────────────────
def afficher_dpe_complet(donnees_dpe):
    """Affiche les données du DPE complet (5 feuilles) de façon structurée."""
    if not donnees_dpe:
        return

    admin   = donnees_dpe.get("administratif", {})
    logem   = donnees_dpe.get("logement", {})
    sortie  = donnees_dpe.get("logement_sortie", {})
    rapport = donnees_dpe.get("rapport", {})

    print(f"\n{SEP2}")
    print(f"  DPE COMPLET — OBSERVATOIRE ADEME (475 variables / 5 feuilles)")
    print(f"{SEP2}")

    # ── Identité du DPE
    print(f"\n  IDENTIFICATION")
    print(f"  {'─'*40}")
    print(f"  Numéro DPE          : {admin.get('reference_interne_projet', '—')}")
    print(f"  Date visite         : {admin.get('date_visite_diagnostiqueur', '—')}")
    print(f"  Date établissement  : {admin.get('date_etablissement_dpe', '—')}")
    print(f"  Modèle DPE          : {admin.get('modele_dpe', '—')}")
    print(f"  Moteur de calcul    : {admin.get('version_moteur_calcul', '—')}")

    # ── Adresse
    print(f"\n  ADRESSE (géocodée BAN)")
    print(f"  {'─'*40}")
    print(f"  Adresse             : {admin.get('ban_housenumber', '')} {admin.get('ban_street', '')}")
    print(f"  Code postal         : {admin.get('ban_postcode', '—')}")
    print(f"  Commune             : {admin.get('ban_city', '—')}")
    print(f"  Code INSEE          : {admin.get('ban_citycode', '—')}")
    print(f"  Score géocodage     : {admin.get('ban_score', '—')}")
    print(f"  Complément          : {admin.get('compl_ref_logement', '—')}")

    # ── Caractéristiques du logement
    print(f"\n  CARACTÉRISTIQUES PHYSIQUES")
    print(f"  {'─'*40}")
    print(f"  Période construction: {logem.get('periode_construction', '—')}")
    print(f"  Surface habitable   : {logem.get('surface_habitable_logement', '—')} m²")
    print(f"  Nb niveaux          : {logem.get('nombre_niveau_logement', '—')}")
    print(f"  Hauteur sous plafond: {logem.get('hsp', '—')} m")
    print(f"  Zone climatique     : {logem.get('zone_climatique', '—')}")
    print(f"  Classe altitude     : {logem.get('classe_altitude', '—')}")
    print(f"  Classe inertie      : {logem.get('classe_inertie', '—')}")
    print(f"  Méthode DPE         : {logem.get('methode_application_dpe_log', '—')}")

    # ── Résultats DPE
    print(f"\n  RÉSULTATS DPE (calcul 3CL réglementaire)")
    print(f"  {'─'*40}")
    etiq_e = sortie.get("classe_bilan_dpe", "—")
    etiq_g = sortie.get("classe_emission_ges", "—")
    conso  = sortie.get("conso_5_usages_m2") or sortie.get("ep_conso_5_usages_m2", "—")
    ges    = sortie.get("emission_ges_5_usages_m2", "—")
    cout   = sortie.get("cout_5_usages", "—")

    print(f"  Étiquette Énergie   : {etiq_e}  ({conso} kWh EP/m²/an)")
    print(f"  Étiquette GES       : {etiq_g}  ({ges} kg CO₂/m²/an)")
    try:
        print(f"  Coût annuel estimé  : {round(float(cout)):,} €/an".replace(",", " "))
    except Exception:
        print(f"  Coût annuel estimé  : {cout} €/an")
    print(f"  Confort d'été       : {sortie.get('indicateur_confort_ete', '—')}")
    print(f"  Ubat (enveloppe)    : {sortie.get('ubat', '—')} W/m².K")

    # ── Qualité isolation
    print(f"\n  QUALITÉ ISOLATION")
    print(f"  {'─'*40}")
    print(f"  Enveloppe globale   : {sortie.get('qualite_isol_enveloppe', '—')}")
    print(f"  Murs                : {sortie.get('qualite_isol_mur', '—')}")
    print(f"  Plancher haut       : {sortie.get('qualite_isol_plancher_haut_comble_amenage', '—')}")
    print(f"  Plancher bas        : {sortie.get('qualite_isol_plancher_bas', '—')}")
    print(f"  Menuiseries         : {sortie.get('qualite_isol_menuiserie', '—')}")

    # ── Déperditions thermiques
    print(f"\n  DÉPERDITIONS THERMIQUES (W/K)")
    print(f"  {'─'*40}")
    deperditions = [
        ("Murs",              "deperdition_mur"),
        ("Plancher bas",      "deperdition_plancher_bas"),
        ("Plancher haut",     "deperdition_plancher_haut"),
        ("Baies vitrées",     "deperdition_baie_vitree"),
        ("Ponts thermiques",  "deperdition_pont_thermique"),
        ("Renouvellement air","deperdition_renouvellement_air"),
        ("TOTAL enveloppe",   "deperdition_enveloppe"),
    ]
    total = sortie.get("deperdition_enveloppe", 0)
    for label, key in deperditions:
        val = sortie.get(key)
        if val is not None:
            try:
                pct = f"  ({round(float(val)/float(total)*100)}%)" if total and key != "deperdition_enveloppe" else ""
                print(f"  {label:<22}: {round(float(val), 1)} W/K{pct}")
            except Exception:
                print(f"  {label:<22}: {val} W/K")

    # ── Énergie principale
    print(f"\n  ÉNERGIE & CONSOMMATIONS")
    print(f"  {'─'*40}")
    print(f"  Type énergie        : {sortie.get('type_energie', '—')}")
    try:
        print(f"  Besoin chauffage    : {round(float(sortie.get('besoin_ch', 0))):,} kWh/an".replace(",", " "))
        print(f"  Besoin ECS          : {round(float(sortie.get('besoin_ecs', 0))):,} kWh/an".replace(",", " "))
        print(f"  Conso chauffage EF  : {round(float(sortie.get('conso_ch', 0))):,} kWh/an".replace(",", " "))
        print(f"  Conso ECS EF        : {round(float(sortie.get('conso_ecs', 0))):,} kWh/an".replace(",", " "))
    except Exception:
        pass

    # ── Recommandations de travaux
    if rapport:
        print(f"\n  RECOMMANDATIONS DE TRAVAUX")
        print(f"  {'─'*40}")
        desc = rapport.get("descriptif_simplifie", [])
        if desc:
            cats = list(dict.fromkeys(desc))  # dédupliquer en gardant l'ordre
            print(f"  Postes à améliorer  : {', '.join(cats[:6])}")

        packs = rapport.get("packs_travaux", [])
        if packs:
            print(f"\n  PACKS DE TRAVAUX PRÉCONISÉS")
            # Récupérer étiquette initiale pour calculer le gain
            conso_initiale = conso
            for pack in packs:
                num   = pack.get("numero", "?")
                conso_a = pack.get("conso_apres_kwhep_m2", "—")
                ges_a   = pack.get("ges_apres_kgco2_m2", "—")
                cout_min = pack.get("cout_min_eur_m2", "—")
                cout_max = pack.get("cout_max_eur_m2", "—")
                travaux  = pack.get("travaux", [])

                print(f"\n  Pack {num} : {' + '.join(travaux)}")
                print(f"    Conso après travaux  : {conso_a} kWh EP/m²/an")
                print(f"    GES après travaux    : {ges_a} kg CO₂/m²/an")
                print(f"    Coût estimé          : {cout_min} – {cout_max} €/m²")

        gestes = rapport.get("gestes_entretien", [])
        if gestes:
            print(f"\n  Gestes d'entretien    : {', '.join(gestes)}")

    print()


# ─────────────────────────────────────────────────────────────────────────────
# TÉLÉCHARGEMENT DPE (API simplifiée + XLS complet)
# ─────────────────────────────────────────────────────────────────────────────
def telecharger_dossier_complet(numero_dpe, dossier_sortie=None):
    """
    Récupère le DPE complet :
    1. Tente le téléchargement XLS (5 feuilles, 475 variables) depuis l'Observatoire
    2. Si succès → analyse et affiche les données complètes
    3. Si échec → fallback sur l'API simplifiée data.ademe.fr (~50 champs)
    """
    if not numero_dpe or numero_dpe in ("None", "—"):
        return {}

    if dossier_sortie is None:
        dossier_sortie = os.path.dirname(os.path.abspath(__file__))

    print(f"\n{SEP2}")
    print(f"  RÉCUPÉRATION DPE COMPLET")
    print(f"{SEP2}")

    # ── Tentative 1 : XLS complet depuis l'Observatoire
    chemin_xlsx = telecharger_xlsx_observatoire(numero_dpe, dossier_sortie)
    if chemin_xlsx:
        donnees_dpe = analyser_dpe_xlsx(chemin_xlsx)
        if donnees_dpe:
            afficher_dpe_complet(donnees_dpe)
            return donnees_dpe

    # ── Tentative 2 : API simplifiée data.ademe.fr (fallback)
    print(f"\n  Fallback → API simplifiée data.ademe.fr")
    print(f"  {'─'*40}")

    bases = [
        ("Logements post-2021",  ADEME_LOG_EXIST),
        ("Tertiaire post-2021",  ADEME_TERT_NEW),
        ("Logements neufs",      ADEME_LOG_NEUF),
        ("Tertiaire avant 2021", ADEME_TERT_OLD),
        ("Logements avant 2021", ADEME_LOG_OLD),
    ]
    filtres = [
        {"qs": f'numero_dpe:"{numero_dpe}"'},
        {"qs": f'Numero_DPE:"{numero_dpe}"'},
        {"q":  numero_dpe},
    ]

    for nom_base, url in bases:
        for params in filtres:
            try:
                r = requests.get(url, params={**params, "size": 1}, headers=HEADERS, timeout=20)
                if r.ok:
                    data = r.json()
                    if data.get("total", 0) > 0:
                        result = data["results"][0]
                        print(f"  Trouvé dans : {nom_base}")

                        # Sauvegarde JSON
                        chemin_json = os.path.join(dossier_sortie, f"DPE_{numero_dpe}.json")
                        with open(chemin_json, "w", encoding="utf-8") as f:
                            json.dump(result, f, indent=2, ensure_ascii=False)
                        print(f"  JSON sauvegardé : {chemin_json}")

                        def champ(*cles):
                            for k in cles:
                                val = result.get(k)
                                if val not in (None, "", "None", "nan"):
                                    return str(val)
                            return "—"

                        print(f"  Étiquette énergie : {champ('classe_consommation_energie','Etiquette_DPE','classe_bilan_dpe')}")
                        print(f"  Étiquette GES     : {champ('classe_estimation_ges','Etiquette_GES','classe_emission_ges')}")
                        print(f"  Consommation EP   : {champ('consommation_energie','Conso_5_usages_ep_m2','conso_5_usages_ep_m2')} kWh/m²/an")
                        print(f"  Date DPE          : {champ('date_etablissement_dpe','Date_etablissement_dpe')}")
                        print(f"\n  Note : API simplifiée (~50 champs vs 475 dans le XLS complet)")
                        print(f"  → Pour le XLS complet : {OBS_BASE}/pub/dpe/{numero_dpe}")
                        return result
            except Exception:
                continue

    print(f"  DPE {numero_dpe} non trouvé dans les bases open data ADEME.")
    print(f"  → Consultez : {OBS_BASE}/pub/dpe/{numero_dpe}")
    return {}


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    print(f"\n{SEP2}\n  AKILA PROSPECT — CARTE D'IDENTITÉ BÂTIMENT \n{SEP2}")

    choix = input("  Cible : [T]ertiaire (Entreprise) ou [P]articulier (Logement) ? [T/p] : ").strip().upper()
    type_cible = "P" if choix == "P" else "T"

    entree = input("  Identifiant RNB, clé BAN ou adresse : ").strip()
    if not entree:
        return

    print(f"\n  [1/3] Verrouillage de la cible...")
    res = resoudre_entree(entree)
    bat_id = res.get("bat_id_bdnb")

    if not bat_id:
        print("\n  ✗ Bâtiment introuvable dans la base BDNB.")
        print("    Vérifiez l'adresse ou essayez avec la clé BAN directe.")
        return

    print(f"  [2/3] Extraction des caractéristiques BDNB...")
    p1 = {"batiment_groupe_id": f"eq.{bat_id}", "limit": 1}
    pm = {"batiment_groupe_id": f"eq.{bat_id}", "order": "millesime.desc", "limit": 4}

    def first(lst):
        return lst[0] if lst else {}

    d_base   = first(bdnb("batiment_groupe",
                           {**p1, "select": "code_commune_insee,libelle_commune_insee,s_geom_groupe,code_iris"}))
    d_usage  = first(bdnb("batiment_groupe_synthese_propriete_usage",
                           {**p1, "select": "usage_principal_bdnb_open"}))
    d_ffo    = first(bdnb("batiment_groupe_ffo_bat",
                           {**p1, "select": "annee_construction,mat_mur_txt,mat_toit_txt,nb_log,nb_niveau,usage_niveau_1_txt"}))
    d_topo   = first(bdnb("batiment_groupe_bdtopo_bat",
                           {**p1, "select": "hauteur_mean,altitude_sol_mean,l_usage_1"}))
    d_prop   = first(bdnb("batiment_groupe_proprietaire",
                           {**p1, "select": "bat_prop_denomination_proprietaire"}))
    d_risque = first(bdnb("batiment_groupe_risques",
                           {**p1, "select": "alea_argile,alea_radon,alea_sismique"}))
    d_reseau = first(bdnb("batiment_groupe_indicateur_reseau_chaud_froid",
                           {**p1, "select": "indicateur_distance_au_reseau,reseau_en_construction"}))

    if type_cible == "T":
        d_dpe = first(bdnb("batiment_groupe_dpe_tertiaire",
                            {**p1, "select": "identifiant_dpe,classe_conso_energie_dpe_tertiaire,"
                                             "classe_emission_ges_dpe_tertiaire,conso_dpe_tertiaire_ep_m2,"
                                             "emission_ges_dpe_tertiaire_m2,type_energie_chauffage,"
                                             "date_etablissement_dpe,surface_utile,shon"}))
    else:
        d_dpe = first(bdnb("batiment_groupe_dpe_representatif_logement",
                            {**p1, "select": "identifiant_dpe,classe_bilan_dpe,classe_emission_ges,"
                                             "conso_5_usages_ep_m2,emission_ges_5_usages_m2,"
                                             "type_energie_chauffage,date_etablissement_dpe,"
                                             "surface_habitable_immeuble"}))

    d_elec = bdnb("batiment_groupe_dle_elec_multimillesime",
                  {**pm, "select": "millesime,conso_tot,nb_pdl_tot"})
    d_gaz  = bdnb("batiment_groupe_dle_gaz_multimillesime",
                  {**pm, "select": "millesime,conso_tot,nb_pdl_tot"})

    numero_dpe = v(d_dpe.get("identifiant_dpe"))

    print(f"  [3/3] Vérification dans l'Open Data (ADEME)...")
    donnees_ademe, erreurs_ademe = collecter_ademe_direct(
        res.get("rnb_id"), res.get("adresse_label"),
        res.get("cle_ban"), type_cible, numero_dpe if numero_dpe != "—" else None
    )

    # ─────────────────────────────────────────────────────────────────────────
    # PARTIE 1 : CARTE D'IDENTITÉ
    # ─────────────────────────────────────────────────────────────────────────
    adresse_affichee = res.get("adresse_label") or "Adresse non résolue"
    print(f"\n{SEP2}")
    print(f"  CARTE D'IDENTITÉ : {adresse_affichee}")
    print(f"{SEP2}")

    print(f"\n  IDENTIFIANTS OFFICIELS")
    print(f"  {'─'*40}")
    print(f"  ID RNB (Bâtiment)        : {v(res.get('rnb_id'))}")
    print(f"  ID BDNB (CSTB)           : {v(bat_id)}")
    print(f"  Clé BAN (Interop)        : {v(res.get('cle_ban'))}")
    if res.get("rnb_id"):
        print(f"  Fiche RNB                : https://rnb.beta.gouv.fr/batiment/{res['rnb_id']}")
    print(f"  Fiche BDNB               : https://bdnb.io/batiment/{bat_id}")

    print(f"\n  LOCALISATION & TERRAIN")
    print(f"  {'─'*40}")
    print(f"  Commune (INSEE)          : {v(d_base.get('code_commune_insee'))} - {v(d_base.get('libelle_commune_insee'))}")
    print(f"  Code IRIS                : {v(d_base.get('code_iris'))}")
    print(f"  Coordonnées GPS          : {v(res.get('lat'))}, {v(res.get('lon'))}")

    print(f"\n  CARACTÉRISTIQUES PHYSIQUES")
    print(f"  {'─'*40}")
    print(f"  Année de construction    : {v(d_ffo.get('annee_construction'))}")
    try:
        age = 2025 - int(d_ffo["annee_construction"])
        print(f"  Age du bâtiment          : {age} ans")
    except Exception:
        pass
    surf = d_base.get("s_geom_groupe")
    print(f"  Surface emprise au sol   : {v(round(float(surf)) if surf else None, 'm²')}")
    print(f"  Hauteur moyenne          : {v(d_topo.get('hauteur_mean'), 'm')}")
    print(f"  Altitude sol             : {v(d_topo.get('altitude_sol_mean'), 'm')}")
    print(f"  Nombre de niveaux        : {v(d_ffo.get('nb_niveau'))}")
    print(f"  Matériaux murs           : {v(d_ffo.get('mat_mur_txt'))}")
    print(f"  Matériaux toit           : {v(d_ffo.get('mat_toit_txt'))}")
    usages_topo = d_topo.get("l_usage_1") or []
    print(f"  Usages BD TOPO           : {v(usages_topo)}")

    print(f"\n  USAGE & PROPRIETE")
    print(f"  {'─'*40}")
    print(f"  Usage principal (BDNB)   : {v(d_usage.get('usage_principal_bdnb_open'))}")
    print(f"  Usage foncier (FF)       : {v(d_ffo.get('usage_niveau_1_txt'))}")
    prop = d_prop.get("bat_prop_denomination_proprietaire") or d_prop.get("l_denomination_proprietaire")
    print(f"  Propriétaire             : {v(prop)}")

    print(f"\n  PERFORMANCE ENERGETIQUE (BDNB)")
    print(f"  {'─'*40}")
    print(f"  Numéro DPE rattaché      : {numero_dpe}")
    if type_cible == "T":
        print(f"  Etiquette Energie        : {v(d_dpe.get('classe_conso_energie_dpe_tertiaire'))}")
        print(f"  Etiquette GES            : {v(d_dpe.get('classe_emission_ges_dpe_tertiaire'))}")
        print(f"  Consommation EP          : {v(d_dpe.get('conso_dpe_tertiaire_ep_m2'), 'kWh/m²/an')}")
        print(f"  Emissions GES            : {v(d_dpe.get('emission_ges_dpe_tertiaire_m2'), 'kg CO2/m²/an')}")
        print(f"  Energie chauffage        : {v(d_dpe.get('type_energie_chauffage'))}")
        print(f"  Surface utile            : {v(d_dpe.get('surface_utile'), 'm²')}")
        print(f"  Date DPE                 : {v(d_dpe.get('date_etablissement_dpe'))}")
    else:
        print(f"  Etiquette Energie        : {v(d_dpe.get('classe_bilan_dpe'))}")
        print(f"  Etiquette GES            : {v(d_dpe.get('classe_emission_ges'))}")
        print(f"  Conso 5 usages EP        : {v(d_dpe.get('conso_5_usages_ep_m2'), 'kWh/m²/an')}")
        print(f"  Emissions GES            : {v(d_dpe.get('emission_ges_5_usages_m2'), 'kg CO2/m²/an')}")
        print(f"  Surface habitable        : {v(d_dpe.get('surface_habitable_immeuble'), 'm²')}")
        print(f"  Date DPE                 : {v(d_dpe.get('date_etablissement_dpe'))}")

    print(f"\n  CONSOMMATIONS RÉELLES (DLE — SDES)")
    print(f"  {'─'*40}")
    print(f"  Electricité :")
    if d_elec:
        vals = []
        for e in d_elec:
            c = e.get("conso_tot")
            vals.append(float(c) if c else 0)
            try:
                cout = f"[~{round(float(c)*0.15/1000):,} k€/an]".replace(",", " ")
            except Exception:
                cout = ""
            print(f"    {e.get('millesime','—')}  →  {round(float(c)):,} kWh  ({v(e.get('nb_pdl_tot'))} PDL)  {cout}".replace(",", " "))
        if len(vals) >= 2:
            moy = sum(vals)/len(vals)
            delta = (vals[0]-vals[-1])/vals[-1]*100 if vals[-1] else 0
            signe = "↓" if delta < 0 else "↑"
            print(f"    Tendance : {signe} {abs(round(delta,1))}%  |  Moyenne : {round(moy):,} kWh/an".replace(",", " "))
    else:
        print("    Non disponible en open data.")
    print(f"  Gaz :")
    if d_gaz:
        for g in d_gaz:
            c = g.get("conso_tot")
            try:
                print(f"    {g.get('millesime','—')}  →  {round(float(c)):,} kWh  ({v(g.get('nb_pdl_tot'))} PDL)".replace(",", " "))
            except Exception:
                print(f"    {g.get('millesime','—')}  →  —")
    else:
        print("    Non disponible.")

    print(f"\n  RISQUES BÂTIMENTAIRES")
    print(f"  {'─'*40}")
    print(f"  Argile / RGA             : {v(d_risque.get('alea_argile'))}")
    print(f"  Radon                    : {v(d_risque.get('alea_radon'))}")
    print(f"  Sismique                 : {v(d_risque.get('alea_sismique'))}")
    if d_reseau:
        print(f"  Réseau chaleur (dist.)   : {v(d_reseau.get('indicateur_distance_au_reseau'))}")
        print(f"  Réseau en construction   : {v(d_reseau.get('reseau_en_construction'))}")

    # ── DPE ADEME Open Data (simplifié)
    def champ(d, *cles):
        for k in cles:
            val = d.get(k)
            if val and val not in (None, "", "None", "nan"):
                return str(val)
        return "—"

    if donnees_ademe:
        print(f"\n  DPE ADEME OPEN DATA ({len(donnees_ademe)} résultat(s))")
        print(f"  {'─'*40}")
        for d in donnees_ademe[:3]:
            addr_parts = [d.get("numero_rue",""), d.get("type_voie",""), d.get("nom_rue",""), d.get("Adresse_Brute","")]
            addr = " ".join(x for x in addr_parts if x and x not in (r"\N", "\\N", "None"))
            cp      = champ(d, "code_postal", "Code_Postal_BAN")
            commune = champ(d, "commune", "Nom_Commune_BAN")
            ndpe    = champ(d, "numero_dpe", "Numero_DPE")
            etiq_e  = champ(d, "classe_consommation_energie", "Etiquette_DPE")
            etiq_g  = champ(d, "classe_estimation_ges", "Etiquette_GES")
            secteur = champ(d, "secteur_activite", "Secteur_activite_principale_batiment", "Type_batiment")
            print(f"  N° DPE : {ndpe}  |  {addr} {cp} {commune}".strip())
            print(f"    Etiq. énergie : {etiq_e}  GES : {etiq_g}  Secteur : {secteur}")
    elif erreurs_ademe:
        print(f"\n  DPE ADEME : erreurs {erreurs_ademe}")

    # ─────────────────────────────────────────────────────────────────────────
    # PARTIE 2 : LIEN OBSERVATOIRE + TÉLÉCHARGEMENT XLS COMPLET
    # ─────────────────────────────────────────────────────────────────────────
    print(f"\n{SEP2}")
    if numero_dpe != "—":
        print(f"  OBSERVATOIRE ADEME — DPE COMPLET")
        print(f"  {'─'*60}")
        print(f"  Numéro DPE : {numero_dpe}")
        print(f"  Lien       : {OBS_BASE}/pub/dpe/{numero_dpe}")
    else:
        print(f"  OBSERVATOIRE ADEME : aucun numéro DPE trouvé.")
    print(f"{SEP2}")

    # ─────────────────────────────────────────────────────────────────────────
    # PARTIE 3 : RAYON X — DUMP BRUT BDNB
    # ─────────────────────────────────────────────────────────────────────────
    print(f"\n{SEP2}")
    print(f"  RAYON X : DONNÉES BRUTES BDNB (tous champs non nuls)")
    print(f"{SEP2}\n")
    all_data = {**d_base, **d_usage, **d_ffo, **d_topo, **d_dpe, **d_risque, **d_reseau}
    if all_data:
        EXCLURE = {"geom_groupe", "geom_groupe_pos_wgs84", "geom_cstr", "geom_adresse"}
        for key, value in sorted(all_data.items()):
            if key not in EXCLURE and value not in (None, "", "None", [], {}):
                print(f"  {key:<45} : {v(value)}")
    else:
        print("  Aucune donnée supplémentaire trouvée.")

    # ─────────────────────────────────────────────────────────────────────────
    # PARTIE 4 : DPE COMPLET (XLS Observatoire ou API fallback)
    # ─────────────────────────────────────────────────────────────────────────
    dossier_script = os.path.dirname(os.path.abspath(__file__))
    donnees_dpe_complet = telecharger_dossier_complet(numero_dpe, dossier_script)

    # ─────────────────────────────────────────────────────────────────────────
    # EXPORT JSON AUTOMATIQUE
    # ─────────────────────────────────────────────────────────────────────────
    slug = slugifier(adresse_affichee)
    ts   = datetime.now().strftime("%Y%m%d_%H%M")
    nom_json = f"akila_{slug}_{ts}.json"
    chemin_json = os.path.join(dossier_script, nom_json)

    export_data = {
        "adresse": adresse_affichee,
        "resolution": res,
        "bdnb": {
            "base": d_base, "usage": d_usage, "ffo": d_ffo, "topo": d_topo,
            "dpe": d_dpe, "risques": d_risque, "reseau": d_reseau,
            "elec": d_elec, "gaz": d_gaz
        },
        "ademe_open": donnees_ademe,
        "dpe_complet": donnees_dpe_complet if isinstance(donnees_dpe_complet, dict) else {},
        "genere_le": datetime.now().isoformat()
    }
    try:
        with open(chemin_json, "w", encoding="utf-8") as f:
            json.dump(export_data, f, ensure_ascii=False, indent=2, default=str)
        print(f"\n  Export JSON → {chemin_json}")
    except Exception as e:
        print(f"\n  Export JSON échoué : {e}")

    print(f"\n{SEP2}\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
