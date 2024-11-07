import mysql.connector
import psycopg2
from datetime import datetime
import html


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

# Étape 1 : Récupérer l'ID de la campagne depuis la table dim_campagnes
nom_campagne = "Test_CRM_Rabah_DB"
pg_cursor.execute("""
    SELECT campagne_id
    FROM public.dim_campagnes
    WHERE nom_campagne = %s;
""", (nom_campagne,))
campagne_id = pg_cursor.fetchone()[0]


# Étape 2 : Récupérer les champs CRM associés à la campagne
pg_cursor.execute(f"SELECT champ_id, campagne_id, nom_champ_campagne, nom_champ_standard, type_entite FROM public.fact_champs WHERE campagne_id = {campagne_id} AND nom_champ_standard not in ('Etat','Statuts','Statut du contact');")
id_crm_champs = pg_cursor.fetchall()
id_crm_champs_list = ', '.join(["'" + str(item[2]) + "'" for item in id_crm_champs])  # Prépare la liste pour la requête SQL

champs_dicto = {t[2]: str(t[0]) for t in id_crm_champs}

# Étape 3 : Exécuter la requête dans Vtiger pour récupérer les modifications
mysql_cursor.execute(f"""
    SELECT
        vtiger_modtracker_basic.id,
        vtiger_modtracker_basic.crmid,  
        vtiger_modtracker_basic.module,
        vtiger_users.id AS modificateur,
        vtiger_modtracker_basic.changedon,
        vtiger_modtracker_detail.fieldname,
        vtiger_modtracker_detail.prevalue,
        vtiger_modtracker_detail.postvalue,
        CASE
            WHEN vtiger_crmentity.setype = 'Accounts' THEN vtiger_crmentity.crmid
            ELSE NULL
        END AS accountid,  
        CASE
            WHEN vtiger_crmentity.setype = 'Contacts' THEN vtiger_crmentity.crmid
            ELSE NULL
        END AS contactid
    FROM
        vtiger_modtracker_detail
    JOIN
        vtiger_modtracker_basic ON vtiger_modtracker_detail.id = vtiger_modtracker_basic.id
    JOIN
        vtiger_users ON vtiger_modtracker_basic.whodid = vtiger_users.id
    JOIN
        vtiger_crmentity ON vtiger_modtracker_basic.crmid = vtiger_crmentity.crmid
    WHERE
        vtiger_modtracker_detail.fieldname IN ({id_crm_champs_list},'firstname','lastname') 
    AND vtiger_crmentity.setype IN ('Accounts', 'Contacts')  
    AND vtiger_modtracker_basic.changedon > '2024-10-18'
    ORDER BY
        vtiger_modtracker_basic.changedon DESC;
""")
modifications = mysql_cursor.fetchall()

# Étape 4 : Insérer les résultats dans la table fact_modifications en évitant les doublons
for modification in modifications:
    id, crmid, module, modificateur, changedon, fieldname, prevalue, postvalue, accountid, contactid = modification
    if fieldname == "lastname" : 
        fieldname = "01" #identifiant correrspondant à nom dans la table fact_champs
    elif fieldname == "firstname": 
        fieldname = "00" 
    modification_id = f"{id}_{crmid}_{fieldname}"
    print(champs_dicto)
    id_champ = champs_dicto.get(fieldname, "ID inconnu")
    # Décodage des entités HTML dans prevalue et postvalue
    prevalue = html.unescape(prevalue) if prevalue else prevalue
    postvalue = html.unescape(postvalue) if postvalue else postvalue

    # Insertion ou mise à jour dans la table fact_modifications
    pg_cursor.execute("""
        INSERT INTO public.fact_modifications 
        (modification_id, campagne_id, commercial_id, compte_id, contact_id, champ_id, valeur_avant, valeur_apres, date_modification)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (modification_id) DO NOTHING;
    """, (modification_id, campagne_id, modificateur, accountid, contactid, id_champ, prevalue, postvalue, changedon))

# Valider les transactions
pg_conn.commit()

# Fermer les connexions
pg_cursor.close()
pg_conn.close()
mysql_cursor.close()
mysql_conn.close()


