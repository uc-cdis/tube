from tube import app, app_init, app_register_blueprints

app_init(app)
app_register_blueprints(app)
app.run(debug=True, port=7500)

