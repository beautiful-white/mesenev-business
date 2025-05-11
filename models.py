from peewee import *

database = SqliteDatabase('space_tourism.db')


class UnknownField(object):
    def __init__(self, *_, **__): pass


class BaseModel(Model):
    class Meta:
        database = database


class Airports(BaseModel):
    name = CharField()

    class Meta:
        table_name = 'airports'


class Positions(BaseModel):
    name = CharField(unique=True)
    salary = IntegerField()

    class Meta:
        table_name = 'positions'


class Candidats(BaseModel):
    mark = IntegerField()
    name = CharField()
    position = ForeignKeyField(
        column_name='position_id', field='id', model=Positions)
    surname = CharField()

    class Meta:
        table_name = 'candidats'


class Clients(BaseModel):
    name = CharField()
    surname = CharField()

    class Meta:
        table_name = 'clients'


class SpaceObjects(BaseModel):
    name = CharField()

    class Meta:
        table_name = 'space_objects'


class Statuses(BaseModel):
    name = CharField(unique=True)

    class Meta:
        table_name = 'statuses'


class Spaceships(BaseModel):
    human_places = IntegerField()
    name = CharField()
    petrol_tank_size = IntegerField()
    status = ForeignKeyField(column_name='status_id',
                             field='id', model=Statuses)

    class Meta:
        table_name = 'spaceships'


class Tours(BaseModel):
    airport = ForeignKeyField(column_name='airport_id',
                              field='id', model=Airports)
    cost = IntegerField()
    date = DateField()
    name = CharField()
    obj = ForeignKeyField(column_name='obj_id', field='id', model=SpaceObjects)
    spaceship = ForeignKeyField(
        column_name='spaceship_id', field='id', model=Spaceships)

    class Meta:
        table_name = 'tours'


class Employees(BaseModel):
    name = CharField()
    position = ForeignKeyField(
        column_name='position_id', field='id', model=Positions)
    surname = CharField()
    tour = ForeignKeyField(column_name='tour_id',
                           field='id', model=Tours, null=True)

    class Meta:
        table_name = 'employees'


class MaintainEmployees(BaseModel):
    name = CharField()
    position = ForeignKeyField(
        column_name='position_id', field='id', model=Positions)
    surname = CharField()
    ticket_id = IntegerField(null=True)

    class Meta:
        table_name = 'maintain_employees'


class Products(BaseModel):
    price = IntegerField()
    product = CharField()

    class Meta:
        table_name = 'products'


class Reasons(BaseModel):
    cost = IntegerField()
    count = IntegerField()
    name = CharField(unique=True)
    product = ForeignKeyField(column_name='product_id',
                              field='id', model=Products)

    class Meta:
        table_name = 'reasons'


class MaintainTickets(BaseModel):
    id = ForeignKeyField(column_name='id', field='ticket_id',
                         model=MaintainEmployees, primary_key=True)
    reason = ForeignKeyField(column_name='reason_id',
                             field='id', model=Reasons)
    spaceship = ForeignKeyField(
        column_name='spaceship_id', field='id', model=Spaceships)

    class Meta:
        table_name = 'maintain_tickets'


class Reviews(BaseModel):
    client = ForeignKeyField(column_name='client_id',
                             field='id', model=Clients)
    date = DateField()
    text = CharField()
    tour = ForeignKeyField(column_name='tour_id', field='id', model=Tours)

    class Meta:
        table_name = 'reviews'
        primary_key = False


class TourTickets(BaseModel):
    client = ForeignKeyField(column_name='client_id',
                             field='id', model=Clients)
    price = IntegerField()
    tour = ForeignKeyField(column_name='tour_id', field='id', model=Tours)

    class Meta:
        table_name = 'tour_tickets'
        primary_key = False
