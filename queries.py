from peewee import *
from models import *


db = SqliteDatabase("./space_tourism.db")
db.connect()


# list(cursor.execute('''
# SELECT
#     t.name AS tour_name,
#     GROUP_CONCAT(DISTINCT t.date) AS tour_date,
#     AVG(tt.price) AS avg_ticket_price,
#     COUNT(tt.client_id) AS client_count
# FROM tours t
# JOIN tour_tickets tt ON t.id = tt.tour_id
# WHERE t.date > '2024-01-01'
# GROUP BY t.name
# HAVING COUNT(tt.client_id) > 2
# ORDER BY avg_ticket_price DESC;

# '''))

query = (Tours
         .select(Tours.name.alias("tour_name"), fn.GROUP_CONCAT(Tours.date.distinct()).alias("tour_data"),
                 fn.AVG(TourTickets.price).alias("avg_ticket_price"),
                 fn.COUNT(TourTickets.client_id).alias("client_count"))
         .join(TourTickets)
         .where(Tours.date > "2024-01-01")
         .group_by(Tours.name)
         .having(fn.COUNT(TourTickets.client) > 2)
         .order_by(SQL('tour_name').desc())

         )

for i in query:
    print(i.tour_name, i.tour_data, i.avg_ticket_price, i.client_count)


# list(cursor.execute('''
# SELECT
#     c.name || ' ' || c.surname AS client_name,
#     r.text AS review_text,
#     r.date AS review_date,
#     t.name AS tour_name,
#     AVG(tt.price) AS avg_tour_price
# FROM clients c
# JOIN reviews r ON c.id = r.client_id
# JOIN tours t ON r.tour_id = t.id
# JOIN tour_tickets tt ON t.id = tt.tour_id
# GROUP BY c.id
# HAVING avg_tour_price > (SELECT AVG(price) FROM tour_tickets)
# ORDER BY avg_tour_price DESC
# LIMIT 10;


# '''))

query = (Clients
         .select((Clients.name & ' ' & Clients.surname).alias("client_name"))
         )

for i in query:
    print(i.client_name)
