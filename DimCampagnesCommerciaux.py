import mysql.connector
import psycopg2
from datetime import datetime

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
campagne_id = pg_cursor.fetchone()

if campagne_id is None:
    print(f"Campagne '{nom_campagne}' non trouvée dans la table dim_campagnes.")
else:
    campagne_id = campagne_id[0]  # Extraire l'ID

    # Étape 2 : Récupérer les commerciaux actifs dans Vtiger liés à cette campagne
    mysql_cursor.execute("""
        SELECT
            users.id AS commercial_id
        FROM
            vtiger_users users
        WHERE
            users.status = 'Active';
    """)
    active_commercials = mysql_cursor.fetchall()

    # Étape 3 : Récupérer les associations existantes dans fact_campagne_commerciaux
    pg_cursor.execute("""
        SELECT commercial_id
        FROM public.fact_campagne_commerciaux
        WHERE campagne_id = %s;
    """, (campagne_id,))
    existing_commercials = pg_cursor.fetchall()

    # Convertir en set pour faciliter la recherche
    existing_set = set([commercial[0] for commercial in existing_commercials])
    active_set = set([commercial[0] for commercial in active_commercials])

    # Étape 4 : Ajouter les nouvelles associations commercial/campagne
    for commercial_id in active_set:
        if commercial_id not in existing_set:
            role_commercial = 'Commercial'
            date_affectation = datetime.now()

            # Insérer la nouvelle association dans fact_campagne_commerciaux
            pg_cursor.execute("""
                INSERT INTO public.fact_campagne_commerciaux (campagne_id, commercial_id, date_affectation, role_commercial)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (campagne_id, commercial_id) DO NOTHING;
            """, (campagne_id, commercial_id, date_affectation, role_commercial))

    # Étape 5 : Supprimer les associations non actives
    for commercial_id in existing_set:
        if commercial_id not in active_set:
            # Supprimer l'entrée de la table PostgreSQL
            pg_cursor.execute("""
                DELETE FROM public.fact_campagne_commerciaux
                WHERE campagne_id = %s AND commercial_id = %s;
            """, (campagne_id, commercial_id))

    # Valider les transactions
    pg_conn.commit()

# Fermer les connexions
pg_cursor.close()
pg_conn.close()
mysql_cursor.close()
mysql_conn.close()