use RESTfulSwarmDB
db.createUser( { user: "admin", pwd: "root", roles: [ { role: "readWrite", db: "RESTfulSwarmDB" } ] } )
