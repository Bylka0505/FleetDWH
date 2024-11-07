import mysql.connector
import psycopg2

# Connexion à la base de données MySQL
mysql_conn = mysql.connector.connect(
    host="192.168.30.10",
    user="builder",
    password="Y2fQm357ZbEg",
    database="Test_CRM_Rabah_DB"
)
mysql_cursor = mysql_conn.cursor()

# Requête pour récupérer les colonnes depuis MySQL pour les comptes
mysql_query_comptes = "SELECT `columnname`, `fieldlabel` FROM vtiger_field WHERE tablename = 'vtiger_accountscf'"
mysql_cursor.execute(mysql_query_comptes)
champ_comptes = mysql_cursor.fetchall()

# Requête pour récupérer les colonnes depuis MySQL pour les contacts
mysql_query_contacts = """
SELECT `columnname`, `fieldlabel` 
FROM `vtiger_field` 
WHERE vtiger_field.fieldlabel IN (
    'Civilité', 'Fonction', 'Telephone portable', 'Statut du contact', 'Date de rappel', 'Optin', 
    'Ligne directe', 'Standard', 'Telephone 2', 'SIRET', 'Email générique', 'Forme juridique', 
    'Sous-activité', 'Lien linkedin', 'Fonction à supprimer', 'Email à supprimer', 
    'Ligne directe à supprimer', 'Telephone portable à supprimer', 'Categorie de fonction', 
    'Nouvelle fonction', 'Nouvelle adresse mail', 'Nouvelle ligne directe', 'Nouveau portable',
    'Statuts Opt out', 'Source contact', 'Telephone portable personnel', 'E-mail', 
    'Téléphone portable', 'Statut fonction', 'Statut Email', 'Statut Ligne directe', 
    'Statut portable', 'statut email générique', 'Etats', 'Statuts'
) 
AND tablename = 'vtiger_contactscf'
"""
mysql_cursor.execute(mysql_query_contacts)
champ_contacts = mysql_cursor.fetchall()

# Connexion à la base de données PostgreSQL
pg_conn = psycopg2.connect(
    host="localhost",
    database="BDD_FLEET",  # Remplace par le nom de ta base PostgreSQL
    user="root",  # Remplace par ton utilisateur PostgreSQL
    password="root"  # Remplace par ton mot de passe PostgreSQL
)
pg_cursor = pg_conn.cursor()

# Récupérer l'ID de la campagne dans PostgreSQL
pg_cursor.execute("SELECT campagne_id FROM dim_campagnes WHERE nom_campagne = 'Test_CRM_Rabah_DB'")
campagne_id = pg_cursor.fetchone()[0]  # Récupère l'id de la campagne

# Insertion des données pour les comptes
for columnname, fieldlabel in champ_comptes:
    print(columnname, fieldlabel)
    pg_cursor.execute(
        """
        INSERT INTO fact_champs (campagne_id, nom_champ_campagne, nom_champ_standard, type_entite)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (campagne_id, nom_champ_campagne, type_entite)
        DO UPDATE SET 
            nom_champ_standard = EXCLUDED.nom_champ_standard,
            nom_champ_campagne = EXCLUDED.nom_champ_campagne
        """,
        (campagne_id, columnname, fieldlabel, 'compte')
    )

# Insertion des données pour les contacts
for columnname, fieldlabel in champ_contacts:
    pg_cursor.execute(
        """
        INSERT INTO fact_champs (campagne_id, nom_champ_campagne, nom_champ_standard, type_entite)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (campagne_id, nom_champ_campagne, type_entite)
        DO UPDATE SET 
            nom_champ_standard = EXCLUDED.nom_champ_standard,
            nom_champ_campagne = EXCLUDED.nom_champ_campagne
        """,
        (campagne_id, columnname, fieldlabel, 'contact')
    )

# Insertion brute des champs supplémentaires "prenom" et "nom" pour les contacts
pg_cursor.execute(
    """
    INSERT INTO fact_champs (campagne_id, nom_champ_campagne, nom_champ_standard, type_entite)
    VALUES (%s, %s, %s, %s), (%s, %s, %s, %s)
    ON CONFLICT (campagne_id, nom_champ_campagne, type_entite)
    DO UPDATE SET 
        nom_champ_standard = EXCLUDED.nom_champ_standard,
        nom_champ_campagne = EXCLUDED.nom_champ_campagne
    """,
    (campagne_id, "00", "Prenom", "contact", campagne_id, "01", "Nom", "contact")
)

# Valider les transactions dans PostgreSQL
pg_conn.commit()

# Fermer les connexions
mysql_cursor.close()
mysql_conn.close()
pg_cursor.close()
pg_conn.close()

print("Importation réussie dans fact_champs.")
