from peewee import *
from models import *
from fastapi import FastAPI
from fastapi.responses import RedirectResponse

db = SqliteDatabase("./space_tourism.db")
db.connect()

api = FastAPI()


@api.get("/")
async def to_docs():
    return RedirectResponse("/docs")


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

    return list(query.dicts())


@api.get("/clients")
async def about_clients(limit: int = 10):
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
             .limit(limit)
             )

    # return [[i.name, i.surname, i.review_text, i.review_date, i.tour_name, i.avg_tour_price] for i in query.namedtuples()]
    return list(query.dicts())


@api.get("/candidates")
async def candidates():
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

    # return [[i.candidate_name, i.position_name, i.candidate_mark, i.avg_position_mark, i.current_employees_in_position] for i in query.namedtuples()]
    return list(query.dicts())


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

    # return [[i.spaceship_name, i.total_maintenance_cost] for i in query]
    return list(query.dicts())


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

    # return [[i.airport_name, i.tour_count, i.space_objects, i.total_passenger_capacity] for i in query]
    return list(query.dicts())


@api.get('/pop_tours')
async def pop_tours():
    tour_count = fn.COUNT(Tours.id).alias('tour_count')
    query = (
        SpaceObjects
        .select(
            SpaceObjects.name.alias('destination'),            tour_count,            fn.COUNT(
                fn.DISTINCT(TourTickets.client_id)).alias('unique_clients')
        )
        .join(Tours)
        .join(TourTickets)
        .group_by(SpaceObjects.name)
        .order_by(tour_count.desc())
    )
    return list(query.dicts())


@api.get("/tours/tours_dates")
async def tours():
    query = (
        Tours
        .select(
            Tours.name.alias("tour"),
            Tours.date.alias("date")
        )
    )
    return list(query.dicts())


@api.get("/employees/unused")
async def unused_employees():
    query = (
        Employees
        .select(
            Employees.name.alias("name"),
            Employees.surname.alias("surname"),
            Employees.position.alias("position")
        )
        .where(
            Employees.tour.is_null()
        )
    )
    return list(query.dicts())


@api.get('/candidates/names')
async def cand_list():
    query = (
        Candidats
        .select(
            Candidats.name.concat('').concat(Candidats.surname).alias(
                'candidate'),            Positions.name.alias('position')
        )
        .join(Positions)
        .group_by(Candidats.id)
    )
    return list(query.dicts())


@api.get('/spaceships')
async def spaceships():
    query = (
        Spaceships
        .select(
            Spaceships.name.alias('name'),            Spaceships.petrol_tank_size.alias('tank_size'),            Spaceships.human_places.alias(
                'seating_capacity'),            Statuses.name.alias('status'),            Airports.name.alias('airport'),            fn.GROUP_CONCAT(Tours.name).alias('tours')
        )
        .join(Tours)
        .switch(Spaceships)
        .join(Statuses)
        .switch(Tours)
        .join(Airports)
        .group_by(Spaceships.id)
    )
    return list(query.dicts())


@api.get('/emp_rank')
async def emp_rank():
    query = (Positions
             .select(
                 Employees.name.concat(' ').concat(Employees.surname).alias('e_name'),                Positions.name.alias('p_name'),                Positions.salary.alias(
                     'p_salary'),                fn.DENSE_RANK().over(order_by=[Positions.salary.desc()]).alias('salary_rank')
             )
             .join(Employees, on=(Positions.id == Employees.position_id))
             )
    return list(query.dicts())


@api.get('/raise')
async def mte_raise():
    need_raise = Case(None, [(fn.COUNT(MaintainTickets.id)
                      > 10, 'Needs a raise')], 'Doesnt need a raise')
    query = (
        Positions
        .select(
            MaintainEmployees.name.concat(' ').concat(MaintainEmployees.surname).alias('candidate_name'),            Positions.name.alias(
                'position'),            fn.COUNT(MaintainTickets.id).alias('actions'),            need_raise.alias('raise_status')
        )
        .join(MaintainEmployees, on=(MaintainEmployees.position_id == Positions.id))
        .join(MaintainTickets, on=(MaintainEmployees.ticket_id == MaintainTickets.id))
        .group_by(MaintainEmployees.id)
    )
    return list(query.dicts())


@api.get('/tours_rev')
async def tours_rev():
    total_revenue = fn.SUM(TourTickets.price).alias('total_revenue')
    query = (
        Tours
        .select(
            Tours.name.alias("tour_name"),            fn.COUNT(
                TourTickets.client_id).alias('tickets'),            total_revenue
        )
        .join(TourTickets, on=(TourTickets.tour == Tours.id))
        .group_by(Tours.id)
        .order_by(total_revenue.desc())
    )
    return list(query.dicts())


@api.get('/pop_obj')
async def pop_obj():
    popularity = fn.COUNT(TourTickets.tour_id).alias('popularity')
    total_revenue = fn.SUM(TourTickets.price).alias('total_revenue')
    query = (
        SpaceObjects
        .select(
            SpaceObjects.name.alias(
                'so_name'),            popularity,            total_revenue
        )
        .join(Tours)
        .join(TourTickets)
        .group_by(SpaceObjects.name)
        .order_by(total_revenue, popularity.desc(), SpaceObjects.name)
    )
    return list(query.dicts())
