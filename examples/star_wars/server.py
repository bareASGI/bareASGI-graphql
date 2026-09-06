from bareasgi import Application
from bareasgi_graphql import add_graphql
from star_wars.star_wars_schema import star_wars_schema

import uvicorn

app = Application()
add_graphql(app, star_wars_schema)

uvicorn.run(app, port=9009)