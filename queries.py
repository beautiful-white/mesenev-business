from peewee import *
from models import *
from fastapi import FastAPI


db = SqliteDatabase("./space_tourism.db")
db.connect()

api = FastAPI()


@api.get("/tours")
async def about_tours():
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

    return [[i.tour_name, i.tour_data, i.avg_ticket_price, i.client_count] for i in query]


@api.get("/clients")
async def about_clients():
    query = (Clients
             .select(Clients.name, Clients.surname,
                     Reviews.text.alias("review_text"),
                     Reviews.date.alias("review_date"),
                     Tours.name.alias("tour_name"),
                     fn.AVG(TourTickets.price).alias("avg_tour_price"))
             .join(Reviews)
             .join(Tours)
             .join(TourTickets)
             .group_by(Clients.id)
             .having(SQL("avg_tour_price") > (TourTickets.select(fn.AVG(TourTickets.price))))
             .order_by(SQL("avg_tour_price").desc())
             .limit(10)
             )

    return [[i.name, i.surname, i.review_text, i.review_date, i.tour_name, i.avg_tour_price] for i in query.namedtuples()]


@api.get("/candidats")
async def candidats():
    Candidat2 = Candidats.alias()

    avg_mark_subq = (Candidat2
                     .select(fn.AVG(Candidat2.mark))
                     .where(Candidat2.position_id == Candidats.position_id))

    query = (Candidats
             .select(
                 Candidats.name.concat(' ').concat(
                     Candidats.surname).alias('candidate_name'),
                 Positions.name.alias('position_name'),
                 Candidats.mark.alias('candidate_mark'),
                 fn.AVG(Candidat2.mark).alias('avg_position_mark'),
                 fn.COUNT(Employees.id).alias('current_employees_in_position')
             )
             .join(Positions, on=(Candidats.position_id == Positions.id))
             .join(Employees, JOIN.LEFT_OUTER, on=(Employees.position_id == Positions.id))
             .join(Candidat2, on=(Candidat2.position_id == Positions.id))
             .group_by(Candidats.id, Positions.name)
             .having(Candidats.mark > avg_mark_subq)
             .order_by(Candidats.mark.desc()))

    return [[i.candidate_name, i.position_name, i.candidate_mark, i.avg_position_mark, i.current_employees_in_position] for i in query.namedtuples()]


@api.get("/maintenance")
async def maintenance():
    subquery = fn.SUM(Products.price * Reasons.count)

    query = (Spaceships
             .select(
                 Spaceships.name.alias("spaceship_name"),
                 subquery.alias("total_maintenance_cost")
             )
             .join(MaintainTickets, on=(Spaceships.id == MaintainTickets.spaceship_id))
             .join(Reasons, on=(MaintainTickets.reason_id == Reasons.id))
             .join(Products, on=(Reasons.product_id == Products.id))
             .group_by(Spaceships.id)
             .order_by(subquery.desc())

             )

    return [[i.spaceship_name, i.total_maintenance_cost] for i in query]


@api.get("/airports")
async def airports():
    tour_count = fn.COUNT(Tours.id)
    query = (
        Airports
        .select(
            Airports.name.alias("airport_name"),
            tour_count.alias("tour_count"),
            fn.GROUP_CONCAT(SpaceObjects.name.distinct()
                            ).alias("space_objects"),
            fn.SUM(Spaceships.human_places).alias(
                "total_passenger_capacity")
        )
        .join(Tours)
        .join(SpaceObjects)
        .switch(Tours)
        .join(Spaceships)
        .group_by(Airports.id)
        .order_by(tour_count.desc())
    )

    return [[i.airport_name, i.tour_count, i.space_objects, i.total_passenger_capacity] for i in query]
