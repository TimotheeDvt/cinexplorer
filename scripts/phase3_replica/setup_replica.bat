@echo off
start "MONGO NODE 1" mongod --replSet rs0 --port 27017 --dbpath ./data/mongo/db-1 --bind_ip localhost
start "MONGO NODE 2" mongod --replSet rs0 --port 27018 --dbpath ./data/mongo/db-2 --bind_ip localhost
start "MONGO NODE 3" mongod --replSet rs0 --port 27019 --dbpath ./data/mongo/db-3 --bind_ip localhost