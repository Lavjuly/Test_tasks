import psycopg2
import pandas as pd

# Подключение к существующей базе данных
connection = psycopg2.connect(user='postgres',
                                  password='Froggyst_1993',
                                  host='localhost',
                                  port='5432',
                                  database='beauty_saloon')

# SQL-запрос
sql_query = """
-- Учитываем связи продажи с лидами
WITH LeadPurchase AS (
    SELECT
        l.lead_id,
        p.purchase_id,
        p.purchase_created_at,
        p.m_purchase_amount,
        ROW_NUMBER() OVER (PARTITION BY l.lead_id ORDER BY (p.purchase_created_at - l.lead_created_at)) AS PurchaseOrder
    FROM leads l
    INNER JOIN purchases p ON l.client_id = p.client_id
    WHERE p.purchase_created_at <= l.lead_created_at + INTERVAL '15 days'
)

SELECT
    ads.created_at AS Дата,
    ads.d_utm_source AS UTM_source,
    ads.d_utm_medium AS UTM_medium,
    ads.d_utm_campaign AS UTM_campaign,
    COUNT(ads.m_clicks) AS Количество_кликов,
    ROUND(SUM(CAST(ads.m_cost AS numeric)), 2) AS Расходы_на_рекламу,
    COUNT(DISTINCT leads.lead_id) AS Количество_лидов,
    COUNT(DISTINCT lp.purchase_id) AS Количество_покупок,
    ROUND(SUM(CAST(lp.m_purchase_amount AS numeric)), 2) AS Выручка_от_продаж,
    ROUND(CAST(SUM(CAST(ads.m_cost AS numeric)) / COUNT(DISTINCT leads.lead_id) AS numeric), 2) AS CPL,
	ROUND(CAST(SUM(CAST(lp.m_purchase_amount AS numeric)) / CAST(SUM(CAST(ads.m_cost AS numeric)) AS numeric) AS numeric), 2) AS ROAS
FROM ads
INNER JOIN leads ON ads.d_utm_source = leads.d_lead_utm_source
    AND ads.d_utm_medium = leads.d_lead_utm_medium
    AND ads.d_utm_campaign = leads.d_lead_utm_campaign
LEFT JOIN LeadPurchase lp ON leads.lead_id = lp.lead_id AND lp.PurchaseOrder = 1
WHERE ads.m_clicks > 0 AND ads.m_cost > 0
GROUP BY Дата, UTM_source, UTM_medium, UTM_campaign
ORDER BY Дата 
"""

# Выполняем SQL-запрос и считываем результат в DataFrame
df = pd.read_sql_query(sql_query, connection)

# Выгружаем отчет
df.to_csv('report.csv', encoding='Windows-1251')