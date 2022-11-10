db.createUser(
  {
    user: "mm",
    pwd: "pulp_prediction",
    roles: [ { role: "readWrite", db: "movielog" } ]
  }