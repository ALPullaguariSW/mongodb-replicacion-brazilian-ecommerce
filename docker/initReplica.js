// Script para inicializar el Replica Set siguiendo las mejores prácticas de MongoDB
// https://www.mongodb.com/docs/manual/tutorial/deploy-replica-set/

print("🔄 Iniciando configuración del Replica Set rs0...");

// Función para verificar si un host está disponible
function isHostAvailable(host) {
  try {
    var result = db.runCommand({ ping: 1 });
    return result.ok === 1;
  } catch (error) {
    return false;
  }
}

// Función para esperar a que un host esté disponible
function waitForHost(host, maxAttempts = 30) {
  print("⏳ Esperando a que " + host + " esté disponible...");
  for (var i = 0; i < maxAttempts; i++) {
    if (isHostAvailable(host)) {
      print("✅ " + host + " está disponible");
      return true;
    }
    print("⏳ Intento " + (i + 1) + "/" + maxAttempts + " - " + host + " aún no está listo");
    sleep(2000); // Esperar 2 segundos entre intentos
  }
  print("❌ Timeout esperando a que " + host + " esté disponible");
  return false;
}

// Lista de miembros del replica set
var members = [
  "mongo-primary:27017",
  "mongo-secondary1:27017", 
  "mongo-secondary2:27017"
];

// Esperar a que todos los miembros estén disponibles
print("🔍 Verificando disponibilidad de todos los miembros...");
for (var i = 0; i < members.length; i++) {
  if (!waitForHost(members[i])) {
    print("❌ Error: No se pudo conectar a " + members[i]);
    quit(1);
  }
}

// Verificar si el replica set ya está inicializado
try {
  var status = rs.status();
  if (status.ok === 1) {
    print("✅ Replica Set rs0 ya está inicializado");
    print("Estado actual del Replica Set:");
    printjson(status);
    quit(0);
  }
} catch (error) {
  print("ℹ️ Replica Set no inicializado, procediendo con la inicialización...");
}

// Inicializar el replica set
print("🚀 Inicializando Replica Set rs0...");
var config = {
  _id: "rs0",
  members: [
    { _id: 0, host: "mongo-primary:27017", priority: 2 },
    { _id: 1, host: "mongo-secondary1:27017", priority: 1 },
    { _id: 2, host: "mongo-secondary2:27017", priority: 1 }
  ]
};

try {
  rs.initiate(config);
  print("✅ Comando rs.initiate() ejecutado correctamente");
} catch (error) {
  print("❌ Error ejecutando rs.initiate(): " + error.message);
  quit(1);
}

// Esperar a que el replica set esté completamente inicializado
print("⏳ Esperando a que el replica set esté completamente inicializado...");
var attempts = 0;
var maxAttempts = 60; // 2 minutos máximo

while (attempts < maxAttempts) {
  try {
    var status = rs.status();
    if (status.ok === 1) {
      print("✅ Replica Set rs0 inicializado correctamente");
      print("Estado del Replica Set:");
      printjson(status);
      
      // Verificar que hay un primario
      var primary = status.members.find(function(member) {
        return member.stateStr === "PRIMARY";
      });
      
      if (primary) {
        print("👑 Nodo primario: " + primary.name);
      } else {
        print("⚠️ Advertencia: No se detectó un nodo primario");
      }
      
      break;
    }
  } catch (error) {
    // El replica set aún no está listo
  }
  
  sleep(2000);
  attempts++;
  print("⏳ Intento " + attempts + "/" + maxAttempts + " - Esperando inicialización...");
}

if (attempts >= maxAttempts) {
  print("❌ Error: Timeout esperando la inicialización del replica set");
  quit(1);
}

print("🎉 Replica Set rs0 configurado exitosamente!");

