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

# Connexion à la base de données PostgreSQL
pg_conn = psycopg2.connect(
    host="localhost",
    database="BDD_FLEET",
    user="root",
    password="root"
)
pg_cursor = pg_conn.cursor()



# Étape 1 : Récupérer le campagne_id depuis PostgreSQL (dim_campagnes)
pg_cursor.execute("SELECT campagne_id FROM dim_campagnes WHERE nom_campagne = 'Test_CRM_Rabah_DB'")
campagne_id = pg_cursor.fetchone()[0]  # Récupère le campagne_id correspondant

# Étape 2 : Récupérer le nom de la colonne correspondant au SIRET et Raison sociale dans fact_champs, en filtrant par pkid_campagnes
pg_cursor.execute("""
    SELECT nom_champ_campagne 
    FROM fact_champs 
    WHERE nom_champ_standard = 'SIRET' 
    AND campagne_id = %s
""", (campagne_id,))
siret_column = pg_cursor.fetchone()[0]  # Récupère la colonne MySQL correspondant au SIRET

pg_cursor.execute("""
    SELECT nom_champ_campagne 
    FROM fact_champs 
    WHERE nom_champ_standard = 'Raison sociale' 
    AND campagne_id = %s
""", (campagne_id,))
raison_sociale_column = pg_cursor.fetchone()[0]  # Récupère la colonne MySQL correspondant à Raison sociale

# Étape 3 : Récupérer tous les id_champ, nom_champ et type_entite depuis fact_champs (PostgreSQL)
pg_cursor.execute("SELECT nom_champ_campagne, nom_champ_standard, type_entite, champ_id FROM fact_champs WHERE campagne_id = %s", (campagne_id,))
champs = pg_cursor.fetchall()

# Convertir en dictionnaire pour un accès plus rapide
champs_dicto = {t[0]: t[3] for t in champs}
# Filtrer les champs par type_entite
compte_columns = [str(champ[0]) for champ in champs if champ[2] == 'compte']  # Champs pour les comptes
contact_columns = [str(champ[0]) for champ in champs if champ[2] == 'contact']  # Champs pour les contacts

# Créer deux requêtes pour les comptes et les contacts
columns_query_comptes = ', '.join(compte_columns)
columns_query_contacts = ', '.join(contact_columns)

# Étape 4 : Construire les requêtes MySQL pour interroger les colonnes correspondantes dans MySQL
mysql_query_comptes = f"""
    SELECT vtiger_accountscf.{siret_column}, accountid, {columns_query_comptes}  -- Utilise la colonne SIRET récupérée dynamiquement
    FROM vtiger_accountscf
    JOIN vtiger_crmentity ON vtiger_accountscf.accountid = vtiger_crmentity.crmid
    WHERE vtiger_crmentity.deleted = 0
"""


# Récupérer toutes les valeurs de la requête MySQL pour les comptes
mysql_cursor.execute(mysql_query_comptes)
results_comptes = mysql_cursor.fetchall()

# Étape 5 : Insérer les données dans la table fact_champs_integration pour les comptes (avec SIRET comme fkid_comptes)
for result in results_comptes:
    siret = result[0]  # Le SIRET du compte est maintenant dans result[0]
    accountid = result[1]
    values = result[2:] # Toutes les valeurs des colonnes extraites de MySQL (vtiger_accountscf)
    for idx, value in enumerate(values):
        nom_champ_campagne = compte_columns[idx]  # Correspond à l'id_champ dans PostgreSQL pour compte
        id_champ = champs_dicto.get(nom_champ_campagne, "ID inconnu")
        if value is not None and value != '' and siret is not None and siret != '':
            pg_cursor.execute(
                """
                INSERT INTO fact_champs_integration (compte_id, campagne_id, champ_id, valeur_champ, crm_accountid)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (siret, campagne_id, id_champ, value, accountid)  # On utilise le SIRET à la place de accountid
            )


# Étape 6 : Insérer les données dans la table fact_champs_integration pour les contacts
# Récupérer toutes les valeurs de la requête MySQL pour les contacts avec SIRET associé
mysql_query_contacts_with_societe = f"""
    SELECT 
        vtiger_contactscf.contactid, 
        vtiger_contactdetails.accountid,
        vtiger_contactdetails.lastname,
        vtiger_contactdetails.firstname,
        vtiger_accountscf.{raison_sociale_column},  -- Récupère le SIRET depuis vtiger_accountscf
        {columns_query_contacts}
    FROM vtiger_contactdetails
    JOIN vtiger_contactscf ON vtiger_contactscf.contactid = vtiger_contactdetails.contactid
    LEFT JOIN vtiger_accountscf ON vtiger_contactdetails.accountid = vtiger_accountscf.accountid  -- Jointure pour récupérer le SIRET
"""

# Exécuter la requête pour récupérer les contacts avec raison sociale
mysql_cursor.execute(mysql_query_contacts_with_societe)
results_contacts = mysql_cursor.fetchall()

# Insérer les données dans la table fact_champs_integration pour les contacts avec SIRET
for result in results_contacts:
    crm_contactid = result[0]
    accountid = result[1]
    lastname = result[2]
    firstname = result[3]
    raison_sociale = result[4]  # Récupérer la raison sociale
    values = result[5:]
    lastname_part = (lastname[:4].upper() + lastname[-2:].upper()) if len(lastname) >= 6 else lastname.upper().ljust(6)[:6]
    raison_sociale_part = (raison_sociale[:4].upper() + raison_sociale[-2:].upper()) if len(raison_sociale) >= 6 else raison_sociale.upper().ljust(6)[:6]
    firstname_part = (firstname[0].upper() + firstname[-1].upper()) if len(firstname) >= 2 else firstname.upper().ljust(2)[:2]

    # Assemblage final
    contactid = f"{lastname_part}{raison_sociale_part}{firstname_part}"

    for idx, value in enumerate(values):
        nom_champ_campagne = compte_columns[idx]  # Correspond à l'id_champ dans PostgreSQL pour contact
        id_champ = champs_dicto.get(nom_champ_campagne, "ID inconnu")  # Correspond à l'id_champ dans PostgreSQL pour contact
        
        if value is not None and value != '':
            try:
                pg_cursor.execute(
                    """
                    INSERT INTO fact_champs_integration (compte_id, campagne_id, champ_id, valeur_champ, contact_id, crm_accountid, crm_contactid)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (siret, campagne_id, id_champ, value, contactid, accountid, crm_contactid)  # Ajouter le SIRET pour les contacts
                )
            except psycopg2.errors.UniqueViolation:
                # Annuler la transaction pour éviter le blocage et passer au suivant
                pg_conn.rollback()
                print(f"Doublon détecté pour (campagne_id={campagne_id}, compte_id={siret}, champ_id={id_champ}). Ligne ignorée.")
                continue  # Passe à la ligne suivante
            except psycopg2.errors.ForeignKeyViolation as e:
                # Annuler la transaction pour pouvoir réessayer
                pg_conn.rollback()
                # Vérifie si c'est bien l'erreur de clé étrangère sur contact_id
                if "contact_id" in str(e):
                    print(f"Contact {contactid} n'existe pas dans dim_contacts, création en cours...")

                    # Insérer le contact dans dim_contacts avec les informations disponibles
                    pg_cursor.execute(
                        """
                        INSERT INTO dim_contacts (contact_id, siret, nom, prenom)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (contactid, siret, lastname, firstname)
                    )
                    pg_conn.commit()  # Confirme l'insertion du contact

                    # Réessaie l'insertion dans fact_champs_integration
                    pg_cursor.execute(
                        """
                        INSERT INTO fact_champs_integration (compte_id, campagne_id, champ_id, valeur_champ, crm_contact_id, crm_accountid)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (siret, campagne_id, id_champ, value, contactid, accountid)
                    )
                    pg_conn.commit()  # Confirme l'insertion dans fact_champs_integration

                else:
                    # Si l'erreur est d'un autre type, raise pour l'identifier
                    raise

# Commit des autres transactions dans PostgreSQL
pg_conn.commit()

# Fermer les connexions
mysql_cursor.close()
mysql_conn.close()
pg_cursor.close()
pg_conn.close()

print("Importation réussie dans fact_champs_integration.")