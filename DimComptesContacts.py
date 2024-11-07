import pandas as pd
from sqlalchemy import create_engine

# Configuration de la connexion PostgreSQL
username = 'root'
password = 'root'
host = 'localhost'
port = '5432'
database = 'BDD_FLEET'

print("connexion à la base de données")
# Créer une URL de connexion
engine = create_engine(f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}')

# Chemin des fichiers Excel
file_societe = 'BDD_FLEET_MZ_societe.xlsx'
file_contacts = 'BDD_FLEET_MZ_contacts.xlsx'

# Mapping des noms de colonnes pour correspondre à PostgreSQL
cols_dim_comptes = {
    'SIRET': 'siret', 'Siren': 'siren', 'Raison sociale': 'raison_sociale', 
    'Nom de domaine': 'nom_de_domaine', 'Sous-domaine': 'sous_domaine', 
    'Compte rattaché': 'compte_rattache', 'Adresse 1': 'adresse_1', 'Adresse 2': 'adresse_2',
    'Code postal': 'code_postal', 'Ville': 'ville', 'Standard': 'standard',
    'Téléphone siège': 'telephone_siege', 'Téléphone 2': 'telephone_2', 
    'Présence contact(s)': 'presence_contacts', 'Privé_Public': 'prive_public', 
    'Présence_mapping?': 'presence_mapping', 'Date création': 'date_creation', 
    'Date modification': 'date_modification', 'Compte_source': 'compte_source', 
    'Classification': 'classification', 'Commentaire': 'commentaire', 
    'Cessation_date': 'cessation_date', 'URL LINKEDIN': 'url_linkedin', 
    'Segment': 'segment', 'Population': 'population', 'GHT_Site support': 'ght_site_support',
    'Marché UGAP': 'marche_ugap', 'Marché UniHA': 'marche_uniha', 
    'Resah_Lastupdated': 'resah_lastupdated', 'Resah_Source': 'resah_source', 
    'Marché RESAH': 'marche_resah', 'Marché CAIH': 'marche_caih', 
    'Sipperec_Lastupdated': 'sipperec_lastupdated', 'SIPPEREC_Source': 'sipperec_source',
    'Marché SIPPEREC': 'marche_sipperec', 'Département': 'departement', 
    'Région': 'region', 'Code NAF': 'code_naf', 'Libellé activité': 'libelle_activite', 
    'Activité': 'activite', 'Sous-activité': 'sous_activite', 
    'Groupe de forme juridique': 'groupe_forme_juridique', 'Groupe': 'groupe', 
    'GROUPE_Source Mapping': 'groupe_source_mapping', 
    'GROUPE_Source sparklane': 'groupe_source_sparklane', 'Siège social': 'siege_social', 
    'Effectifs société': 'effectifs_societe', 'Tranche effectif société': 'tranche_effectif_societe', 
    'Effectifs consolidés': 'effectifs_consolides', 
    'Tranche effectif société consolidés': 'tranche_effectif_consolides', 
    'Effectif site': 'effectif_site', "Nombre d'établissements": 'nombre_etablissements', 
    "Chiffre d'affaires société": 'chiffre_affaires_societe', 
    "Chiffre d'affaires consolidé": 'chiffre_affaires_consolide'
}

# Colonnes booléennes et colonnes numériques à traiter
boolean_columns = ['presence_contacts', 'presence_mapping', 'marche_ugap', 'marche_uniha', 'marche_resah', 'marche_caih', 'marche_sipperec']
numeric_columns = ['effectifs_societe', 'effectifs_consolides', 'effectif_site', 'nombre_etablissements', 'chiffre_affaires_societe', 'chiffre_affaires_consolide']

# Fonction d'insertion avec conversion des colonnes booléennes et nettoyage des valeurs non définies
def load_excel_to_postgres(file_path, table_name, columns, engine, boolean_columns, numeric_columns):
    # Lire le fichier Excel
    df = pd.read_excel(file_path)
    # Renommer les colonnes
    df.rename(columns=columns, inplace=True)
    
    # Remplacer les occurrences de "Non défini" par NaN dans tout le DataFrame
    df.replace("Non défini", pd.NA, inplace=True)
    
    # Convertir les colonnes booléennes de "Oui"/"Non" en True/False
    for col in boolean_columns:
        if col in df.columns:
            df[col] = df[col].map({'Oui': True, 'Non': False, None: None})
    
    # Convertir les colonnes numériques en s'assurant que "Non défini" est bien remplacé
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')  # Convertit "Non défini" ou autres textes en NaN
    
    # Insérer les données dans PostgreSQL
    df.to_sql(table_name, engine, if_exists='append', index=False)
    print(f"Données insérées dans la table {table_name} depuis {file_path}.")

# Exécuter l'insertion
try:
    load_excel_to_postgres(file_societe, 'dim_comptes', cols_dim_comptes, engine, boolean_columns, numeric_columns)
except Exception as e:
    print("Une erreur s'est produite :", e)
finally:
    engine.dispose()