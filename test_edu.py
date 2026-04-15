import requests, math

lat, lon, rayon = 47.349439, 5.040932, 500
lat_delta = rayon / 111000
lon_delta = rayon / (111000 * math.cos(math.radians(lat)))

r = requests.get(
    "https://data.education.gouv.fr/api/explore/v2.1/catalog/datasets/fr-en-annuaire-education/records",
    params={
        "where": f"latitude >= {lat-lat_delta} AND latitude <= {lat+lat_delta} AND longitude >= {lon-lon_delta} AND longitude <= {lon+lon_delta} AND etat = 'OUVERT'",
        "limit": 10,
        "select": "nom_etablissement,libelle_nature,type_etablissement,adresse_1,code_postal,statut_public_prive,latitude,longitude,telephone"
    }
)
print(f"Status: {r.status_code}")
d = r.json()
print(f"Total: {d.get('total_count', 0)}")
for rec in d.get("results", []):
    print(f"  [{rec.get('type_etablissement')}] {rec.get('nom_etablissement')} — {rec.get('libelle_nature')} — {rec.get('adresse_1')} {rec.get('code_postal')}")
