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

# Récupérer tous les utilisateurs (actifs et inactifs) de Vtiger
mysql_cursor.execute("""
    SELECT
        id AS commercial_id,
        user_name AS nom_commercial,
        first_name AS prenom_commercial,
        email1 AS email_commercial,
        is_admin AS is_admin  -- 'ON' pour admin, 'OFF' pour non-admin
    FROM
        vtiger_users;
""")
users = mysql_cursor.fetchall()
print(users)
# Insertion des utilisateurs dans PostgreSQL
for user in users:
    commercial_id, nom_commercial, prenom_commercial, email_commercial, is_admin = user
    is_admin_boolean = True if is_admin == 'on' else False  # Conversion 'ON' -> True, 'OFF' -> False

    pg_cursor.execute("""
        INSERT INTO public.dim_commerciaux (commercial_id, nom_commercial, prenom_commercial, email_commercial, is_admin)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (commercial_id) 
        DO UPDATE SET 
            nom_commercial = EXCLUDED.nom_commercial,
            prenom_commercial = EXCLUDED.prenom_commercial ,
            email_commercial = EXCLUDED.email_commercial,
            is_admin = EXCLUDED.is_admin;  -- Met à jour en cas de conflit
    """, (commercial_id, nom_commercial, prenom_commercial, email_commercial, is_admin_boolean))


# Valider la transaction après l'insertion des utilisateurs
pg_conn.commit()

# Fermer les connexions
pg_cursor.close()
pg_conn.close()
mysql_cursor.close()
mysql_conn.close()