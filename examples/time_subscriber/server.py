"""Time Subscriber Server"""

from bareasgi import Application
from bareasgi_graphql import add_graphql
from time_subscriber.time_schema import schema

import uvicorn

app = Application()
add_graphql(app, schema)

uvicorn.run(app, port=9009)
