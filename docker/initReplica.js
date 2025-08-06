// Script para inicializar el Replica Set
rs.initiate({
  _id: "rs0",
  members: [
    { _id: 0, host: "mongo-primary:27017", priority: 2 },
    { _id: 1, host: "mongo-secondary1:27017", priority: 1 },
    { _id: 2, host: "mongo-secondary2:27017", priority: 1 }
  ]
});

// Esperar a que el replica set esté completamente inicializado
while (rs.status().ok !== 1) {
  sleep(1000);
}

print("Replica Set rs0 inicializado correctamente");
print("Estado del Replica Set:");
printjson(rs.status());

